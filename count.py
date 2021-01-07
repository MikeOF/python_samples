import logging
import os
import shutil
import subprocess
import time
from pathlib import Path

from schammer import utils
from schammer.component.file_collection import FileCollectionFactory
from schammer.component.naming.analysis import AnalysisTypeNames
from schammer.component.s3.location import S3PrefixLocation, S3LocationFactory
from schammer.component.s3.s3 import S3
from schammer.context.configuration import Configuration
from schammer.context.enums import TaskType, Protocol
from schammer.task.base.ec2 import BaseEC2ShutdownTaskDefinition


class CellrangerCountFastqSource:

    def __init__(self, name: str, protocol: Protocol, s3_location: S3PrefixLocation):

        utils.check_types([('name', name, str), ('s3_location', s3_location, S3PrefixLocation),
                           ('protocol', protocol, Protocol)])
        utils.check_for_empty_strings([('name', name)])

        if not protocol.is_10x_count_gex():
            raise RuntimeError(f'a non-count-gex protocol was passed, {protocol.name}')

        self.name = name
        self.protocol = protocol
        self.s3_location = s3_location

    def __hash__(self):
        return hash(self.name)

    @staticmethod
    def from_json_dict(json_dict: dict):

        s3_location = S3PrefixLocation.from_json_dict(json_dict['s3_location'])

        return CellrangerCountFastqSource(
            name=json_dict['name'],
            protocol=Protocol[json_dict['protocol']],
            s3_location=s3_location
        )

    def to_json_dict(self):

        return {
            'name': self.name,
            'protocol': self.protocol.name,
            's3_location': self.s3_location.to_json_dict()
        }


class CellrangerCountTaskDef(BaseEC2ShutdownTaskDefinition):

    task_type = TaskType.CELLRANGER_COUNT

    def __init__(self, project: str, sample: str, name_tag: str, fastq_sources: list,
                 reference_s3_location: S3PrefixLocation, cellranger_count_args: list):

        # validate input
        utils.check_types([('project', project, str),
                           ('sample', sample, str),
                           ('name_tag', name_tag, str),
                           ('fastq_sources', fastq_sources, list),
                           ('reference_s3_location', reference_s3_location, S3PrefixLocation),
                           ('cellranger_count_args', cellranger_count_args, list)])

        utils.check_types([('fastq_source', s, CellrangerCountFastqSource) for s in fastq_sources])

        utils.check_for_empty_strings([('project', project),
                                       ('sample', sample),
                                       ('name_tag', name_tag)])

        utils.check_types([('cellranger_count_arg', a, str) for a in cellranger_count_args])

        # check for duplicate sources
        if len({s for s in fastq_sources}) != len(fastq_sources):
            raise RuntimeError(f'duplicate fastq sources are not allowed for a cellranger count task, '
                               f'{[s.name for s in fastq_sources]}')

        # check for no fastq sources
        if len(fastq_sources) == 0:
            raise RuntimeError('cannot create a cellranger count task def without fastq sources')

        self.project = project
        self.sample = sample
        self.name_tag = name_tag

        self.fastq_sources = fastq_sources
        self.reference_s3_location = reference_s3_location
        self.cellranger_count_args = cellranger_count_args

    def get_prefix_component_list(self):
        return [self.project, self.sample, self.name_tag, self.task_type.name]

    @staticmethod
    def _from_json_dict(json_dict: dict):

        project = json_dict['project']
        sample = json_dict['sample']
        name_tag = json_dict['name_tag']

        fastq_sources = [CellrangerCountFastqSource.from_json_dict(s) for s in json_dict['fastq_sources']]
        reference_s3_location = S3PrefixLocation.from_json_dict(json_dict['reference_s3_location'])

        cellranger_count_args = list()
        if 'cellranger_count_args' in json_dict:
            cellranger_count_args = json_dict['cellranger_count_args']

        return CellrangerCountTaskDef(project=project,
                                      sample=sample,
                                      name_tag=name_tag,
                                      fastq_sources=fastq_sources,
                                      reference_s3_location=reference_s3_location,
                                      cellranger_count_args=cellranger_count_args)

    def _to_json_dict(self):

        json_dict = dict()

        json_dict['project'] = self.project
        json_dict['sample'] = self.sample
        json_dict['name_tag'] = self.name_tag

        json_dict['fastq_sources'] = [s.to_json_dict() for s in self.fastq_sources]
        json_dict['reference_s3_location'] = self.reference_s3_location.to_json_dict()
        json_dict['cellranger_count_args'] = self.cellranger_count_args

        return json_dict

    def _run_task(self):

        """
        Run the cellranger count process
        :return: nothing
        """

        # get the S3 client and config
        s3 = S3(Configuration())

        # define directories
        fastq_path = Path('.').absolute().joinpath('fastq')
        cellranger_count_output_path = Path('.').absolute().joinpath('cellranger_count_ouput')
        reference_dir_path = Path('.').absolute().joinpath('reference')

        # make directories
        for d in [fastq_path, cellranger_count_output_path, reference_dir_path]:
            d.mkdir()

        # collect / download fastq sets
        logging.info('downloading fastqs')
        fq_collection_list = list()
        for count_fq_source in self.fastq_sources:
            assert isinstance(count_fq_source, CellrangerCountFastqSource)

            # make a dir for this source
            dir_path = fastq_path.joinpath(count_fq_source.name)
            dir_path.mkdir()

            fq_collection = FileCollectionFactory.for_sample_fastq_collection(
                project=self.project,
                sample=self.sample,
                protocol=count_fq_source.protocol,
                source=count_fq_source.name,
                dir_path=dir_path,
                s3_prefix_location=count_fq_source.s3_location
            )

            # download fastq files for this source
            fq_collection.download_from_s3(s3)

            fq_collection_list.append(fq_collection)

        # log the downloaded fastqs
        logging.info('downloaded fastqs:')
        fastq_path_list = list()
        for fq_collection in fq_collection_list:
            fastq_path_list.extend([fp for fp in fq_collection.dir_path.rglob('*') if fp.is_file()])
        for file_path in sorted(fastq_path_list):
            logging.info(f'\t{file_path.relative_to(fastq_path)}')

        # download cellranger reference
        logging.info('downloading genome reference')

        reference_dir_path = Path('.').absolute().joinpath('reference', self.reference_s3_location.get_name())
        reference_dir_path.mkdir(parents=True)

        ref_file_collection = FileCollectionFactory.for_default(
            s3_prefix_location=self.reference_s3_location,
            dir_path=reference_dir_path
        )
        ref_file_collection.download_from_s3(s3)

        # now make the cellranger command and run it
        fastqs_arg = ','.join([str(fqs.dir_path) for fqs in fq_collection_list])

        cellranger_count_ouput_path = Path('.').absolute().joinpath('cellranger_count_output')
        cellranger_count_ouput_path.mkdir()

        cmd_list = ['cellranger', 'count']

        cmd_list.extend(self.cellranger_count_args)

        cmd_list.extend([
            f'--id={self.sample}',
            f'--sample={self.sample}',
            f'--transcriptome={str(reference_dir_path)}',
            f'--fastqs={fastqs_arg}',
            f'--localcores={os.cpu_count()}',
            '--localmem=58',
            '--disable-ui'
        ])

        # add the include introns option if we have any nuclei protocols
        for fastq_source in self.fastq_sources:
            assert isinstance(fastq_source, CellrangerCountFastqSource)

            if fastq_source.protocol.is_10x_nuclei():
                cmd_list.append('--include-introns')

        try:

            subprocess.run(cmd_list, cwd=str(cellranger_count_ouput_path), check=True)

        except subprocess.CalledProcessError as e:

            # try to locate the error file and log its contents
            logging.error('cellranger threw an error, searching for the contents')

            try:

                stage_dir_path = cellranger_count_ouput_path.joinpath(self.sample, 'SC_RNA_COUNTER_CS')

                error_path_count = 1
                for error_file_path in [p for p in stage_dir_path.rglob('_errors') if p.is_file()]:

                    # upload to S3
                    error_file_s3_location = S3LocationFactory.for_task_error_file(
                        prefix_component_list=self.get_prefix_component_list(),
                        name='-'.join((error_file_path.name, str(error_path_count))))

                    s3.upload_file(s3_key_location=error_file_s3_location, path=error_file_path)

                    # log the error
                    with error_file_path.open() as inf:
                        for line in inf:
                            logging.error(line.strip())

                    error_path_count += 1

                # upload the mri file
                mri_file_path = cellranger_count_ouput_path.joinpath(self.sample, self.sample).with_suffix('.mri.tgz')
                error_file_s3_location = S3LocationFactory.for_task_error_file(
                    prefix_component_list=self.get_prefix_component_list(),
                    name=mri_file_path.name)

                s3.upload_file(s3_key_location=error_file_s3_location, path=mri_file_path)

            except Exception as sub_e:
                logging.error(sub_e)

            finally:

                # rethrow the original
                raise e

        # remove stage directory
        stage_dir_path = cellranger_count_ouput_path.joinpath(self.sample, 'SC_RNA_COUNTER_CS')
        if not stage_dir_path.is_dir():
            raise RuntimeError(f'could not find the stage directory, {stage_dir_path}')
        shutil.rmtree(stage_dir_path)

        # upload analysis files
        logging.info('uploading analysis files')

        # clear any files from a previous run of this analysis
        analysis_s3_location = S3LocationFactory.for_sample_analysis(
            project=self.project,
            sample=self.sample,
            name_tag=self.name_tag,
            analysis_type_name=AnalysisTypeNames.cellranger_count
        )

        s3.delete_objects_with_prefix(s3_prefix_location=analysis_s3_location)

        # create file set and perform upload
        analysis_file_collection = FileCollectionFactory.for_default(
            s3_prefix_location=analysis_s3_location,
            dir_path=cellranger_count_ouput_path.joinpath(self.sample)
        )

        analysis_file_collection.upload_to_s3(s3=s3)

        # sleep for good measure
        time.sleep(120)
        logging.info('process complete')
