from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler
)

from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_entity_to_container,
    gen_containers,
)

from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    Source,
    TestableSource,
    TestConnectionReport,
)

from pydantic import Field

from qlik_sdk import (Auth, 
                    AuthType, 
                    Config, 
                    Qlik)

class QlikCloudConnectionConfig(ConfigModel):
    token_secret: str = Field(description="Qlik Sense Cloud API Token. Can be retrieve at https://help.qlik.com/en-US/cloud-services/Subsystems/Hub/Content/Sense_Hub/Admin/mc-generate-api-keys.htm#:~:text=Click%20your%20profile%20in%20the,the%20API%20key%20should%20expire.")
    tenant_url: str = Field(
        description="Url to your Qlik Cloud Tenant: `https://mycompany.[eu|us|as].qlikcloud.com`"
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Unique relationship between the Qlik Cloud tenant and site",
    )

    def make_qlik_cloud_client(self) -> Qlik:
        # https://pypi.org/project/qlik-sdk/#authentication-options

        qlikClient = Qlik(config=Config(host=self.tenant_url, 
                              auth_type=AuthType.APIKey, 
                              api_key=self.token_secret))
        return qlikClient

class QlikCloudConfig(QlikCloudConnectionConfig):
    
    def __init__():
        super()


@dataclass
class QlikApp:
    id: str
    name: str
    description: str
    path: List[str]

@dataclass
class QlikDataset:
    id: str
    name: str
    description: str

@dataclass
class QlikviewApp:
    id: str
    name: str
    description: str

@dataclass
class QlikAutomation:
    id: str
    name: str
    description: str

class ItemKey(ContainerKey):
    item_id: str

class QlikCloudSource(Source):

    source_config: QlikCloudConnectionConfig
    report: SourceReport = SourceReport()

    all_app_map: Dict[str, QlikApp] = {}
    all_dataset_map: Dict[str, QlikDataset] = {}
    all_qvapp_map = Dict[str, QlikviewApp] = {}
    all_automation_map = Dict[str, QlikAutomation] = {}

    platform = 'qlik_cloud'

    def __init__(self, config: QlikCloudConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self._authenticate() # Connection to Qlik Cloud Tenant via SDK


    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            source_config = QlikCloudConnectionConfig.parse_obj_allow_extras(config_dict)
            source_config.make_qlik_cloud_client()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = QlikCloudConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]


    def _authenticate(self) -> None:
        try:
            self.server = self.config.make_qlik_cloud_client()
        except ValueError as e:
            self.report.report_failure(
                key="qlik-cloud-login",
                reason=str(e),
            )

    def _get_all_items(self):
        """
            Method to get all Qlik Items available
        """

        itemsList = self.server.items.get_items()
        for item in itemsList.pagination:
            if item.resourceType == 'app':
                self.all_app_map.append(QlikApp())
            elif item.resourceType == 'dataset':
                self.all_dataset_map.append(QlikDataset())
            elif item.resourceType == 'qvapp':
                self.all_qvapp_map.append(QlikviewApp())
            elif item.resourceType == 'automation':
                self.all_automation_map.append(QlikAutomation())

    def close(self) -> None:
        self.server = None

    def emit_app_containers(self):
        """
        
            Converting apps maps scanned to containers
        
        """
        
        #Processing Qlik Sense Analytics Apps
        for app in self.all_app_map:
            yield from gen_containers(
                    container_key=self.gen_item_key(app.id),
                    name=app.name,
                    description= "" if app.description is None else app.description,
                    sub_types=["Dashboard"]
                )
        
        #Processing Qlikview Analytics Apps
        for app in self.all_qvapp_map:
            yield from gen_containers(
                    container_key=self.gen_item_key(app.id),
                    name=app.name,
                    description= "" if app.description is None else app.description,
                    sub_types=["Dashboard"]
                )

    def emit_automation_containers(self):
        """
        
            Converting automation maps scanned to containers
        
        """

        #Processing Automation
        for automation in self.all_automation_map:
            yield from gen_containers(
                    container_key=self.gen_item_key(automation.id),
                    name=automation.name,
                    description= "" if automation.name is None else automation.name,
                    sub_types=["Automation"]
                )
        
    def emit_datasets_containers(self):
        """
        
            Converting datasets maps scanned to containers
        
        """

        #Processing Dataset
        for dataset in self.all_dataset_map:
            yield from gen_containers(
                    container_key=self.gen_item_key(dataset.id),
                    name=dataset.name,
                    description= "" if dataset.description is None else dataset.description,
                    sub_types=["Dataset"]
                )

    def gen_item_key(self, item_id: str) -> ItemKey:
        return ItemKey(
            platform=self.platform,
            instance=self.source_config.platform_instance,
            item_id=item_id,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        
        if self.server is None:
            return

        try:
            #Requesting and tidying items from Qlik Sense Cloud
            self._get_all_items()
        except Exception as e:
            self.report.report_failure(
                                       key="qlik-sense-cloud-metadata",
                                       reason=f"Unable to retrieve metadata from Qlik Sense Cloud. Information {str(e)}",
                                       )
            
        try:
            #Emitting items to Data Hub
            self.emit_app_containers()
            self.emit_automation_containers()
            self.emit_datasets_containers()
        except Exception as e:
            self.report.report_failure(
                                       key="qlik-sense-cloud-emission",
                                       reason=f"Unable to emit metadata to Data Hub. Information {str(e)}",
                                       )

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass