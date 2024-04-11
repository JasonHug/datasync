package com.cnso.flinkcdc.factory;

import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.model.ETableRelation;
import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.service.impl.AbnormalEnterprisesServiceImpl;
import com.cnso.flinkcdc.service.impl.AbnormalServiceImpl;
import com.cnso.flinkcdc.service.impl.AddressServiceImpl;
import com.cnso.flinkcdc.service.impl.AdminDivisionCodeChangeHistoryServiceImpl;
import com.cnso.flinkcdc.service.impl.AdminDivisionCodeServiceImpl;
import com.cnso.flinkcdc.service.impl.AdministrativePunishmentServiceImpl;
import com.cnso.flinkcdc.service.impl.AdvancedTechnologyServiceServiceImpl;
import com.cnso.flinkcdc.service.impl.ArticleContentServiceImpl;
import com.cnso.flinkcdc.service.impl.ArticleEnterpriseServiceImpl;
import com.cnso.flinkcdc.service.impl.ArticleEnterpriseTagsServiceImpl;
import com.cnso.flinkcdc.service.impl.ArticleMiningServiceImpl;
import com.cnso.flinkcdc.service.impl.AuctionsRelationsServiceImpl;
import com.cnso.flinkcdc.service.impl.BestBriefServiceImpl;
import com.cnso.flinkcdc.service.impl.BestStockInfoServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2016ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2017ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2018ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2019ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2020ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2021ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2022ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2023ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContent2024ServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingContentOtherServiceImpl;
import com.cnso.flinkcdc.service.impl.BiddingRelatedServiceImpl;
import com.cnso.flinkcdc.service.impl.BranchesServiceImpl;
import com.cnso.flinkcdc.service.impl.BuildingEngineeringServiceImpl;
import com.cnso.flinkcdc.service.impl.BuildingServiceImpl;
import com.cnso.flinkcdc.service.impl.BusinessConcessionServiceImpl;
import com.cnso.flinkcdc.service.impl.BusinessIncubatorServiceImpl;
import com.cnso.flinkcdc.service.impl.CasesRelationsServiceImpl;
import com.cnso.flinkcdc.service.impl.CasesServiceImpl;
import com.cnso.flinkcdc.service.impl.CccServiceImpl;
import com.cnso.flinkcdc.service.impl.CertificateMainServiceImpl;
import com.cnso.flinkcdc.service.impl.ChangeRecordsServiceImpl;
import com.cnso.flinkcdc.service.impl.CheckupsServiceImpl;
import com.cnso.flinkcdc.service.impl.ChineseMedicineRecordServiceImpl;
import com.cnso.flinkcdc.service.impl.ClearAccountServiceImpl;
import com.cnso.flinkcdc.service.impl.CompanyFinanceServiceImpl;
import com.cnso.flinkcdc.service.impl.CompanyProductsServiceImpl;
import com.cnso.flinkcdc.service.impl.CompanySimilarsServiceImpl;
import com.cnso.flinkcdc.service.impl.ComputerProjectManagerServiceImpl;
import com.cnso.flinkcdc.service.impl.CopyrightsRelationsServiceImpl;
import com.cnso.flinkcdc.service.impl.CosmeticNonspecialServiceImpl;
import com.cnso.flinkcdc.service.impl.CosmeticProduceLicenseServiceImpl;
import com.cnso.flinkcdc.service.impl.CosmeticSpecialServiceImpl;
import com.cnso.flinkcdc.service.impl.CountryCodeServiceImpl;
import com.cnso.flinkcdc.service.impl.CreditNoIndexFullServiceImpl;
import com.cnso.flinkcdc.service.impl.CreditimportexportDataServiceImpl;
import com.cnso.flinkcdc.service.impl.CurrencyCodeServiceImpl;
import com.cnso.flinkcdc.service.impl.DomainsAllsServiceImpl;
import com.cnso.flinkcdc.service.impl.DoubleCheckupsServiceImpl;
import com.cnso.flinkcdc.service.impl.DrugBusinessServiceImpl;
import com.cnso.flinkcdc.service.impl.DrugPermitCnServiceImpl;
import com.cnso.flinkcdc.service.impl.DrugProduceServiceImpl;
import com.cnso.flinkcdc.service.impl.DrugRecordServiceImpl;
import com.cnso.flinkcdc.service.impl.EconKindCodeServiceImpl;
import com.cnso.flinkcdc.service.impl.ElectronicCertificationAgencyServiceImpl;
import com.cnso.flinkcdc.service.impl.EmailsServiceImpl;
import com.cnso.flinkcdc.service.impl.EmployeesAllsServiceImpl;
import com.cnso.flinkcdc.service.impl.EnterpriseServiceImpl;
import com.cnso.flinkcdc.service.impl.EquityqualityServiceImpl;
import com.cnso.flinkcdc.service.impl.EstateEualificationServiceImpl;
import com.cnso.flinkcdc.service.impl.ExecutedpersonsServiceImpl;
import com.cnso.flinkcdc.service.impl.ExecutionsServiceImpl;
import com.cnso.flinkcdc.service.impl.FinancialServiceImpl;
import com.cnso.flinkcdc.service.impl.FirefightingSystemServiceImpl;
import com.cnso.flinkcdc.service.impl.FoodAdditivesServiceImpl;
import com.cnso.flinkcdc.service.impl.FoodLicenseServiceImpl;
import com.cnso.flinkcdc.service.impl.FoodProductEntServiceImpl;
import com.cnso.flinkcdc.service.impl.FoodProductLicenseServiceImpl;
import com.cnso.flinkcdc.service.impl.ForestryPermitServiceImpl;
import com.cnso.flinkcdc.service.impl.GameServiceImpl;
import com.cnso.flinkcdc.service.impl.GeneralTaxpayerServiceImpl;
import com.cnso.flinkcdc.service.impl.GoudiInformationServiceImpl;
import com.cnso.flinkcdc.service.impl.HazardousChemicalsLicenceServiceImpl;
import com.cnso.flinkcdc.service.impl.HightechEnterprisesServiceImpl;
import com.cnso.flinkcdc.service.impl.HistoryNamesServiceImpl;
import com.cnso.flinkcdc.service.impl.HistoryOperNamesServiceImpl;
import com.cnso.flinkcdc.service.impl.HistoryRegistCapisServiceImpl;
import com.cnso.flinkcdc.service.impl.HuanbaochufasServiceImpl;
import com.cnso.flinkcdc.service.impl.HugeTaxPunishmentServiceImpl;
import com.cnso.flinkcdc.service.impl.HygieneProductServiceImpl;
import com.cnso.flinkcdc.service.impl.IndustryCodeServiceImpl;
import com.cnso.flinkcdc.service.impl.IndustryProduceServiceImpl;
import com.cnso.flinkcdc.service.impl.IndustryVolunteerServiceImpl;
import com.cnso.flinkcdc.service.impl.InnovativeEnterprisesServiceImpl;
import com.cnso.flinkcdc.service.impl.InternetDrugServiceImpl;
import com.cnso.flinkcdc.service.impl.InvestInfoServiceImpl;
import com.cnso.flinkcdc.service.impl.InvestmentEventsServiceImpl;
import com.cnso.flinkcdc.service.impl.InvestmentsServiceImpl;
import com.cnso.flinkcdc.service.impl.InvisibleChampionServiceImpl;
import com.cnso.flinkcdc.service.impl.ItSafeServiceImpl;
import com.cnso.flinkcdc.service.impl.JobTitleCodeServiceImpl;
import com.cnso.flinkcdc.service.impl.JudicialFreezesServiceImpl;
import com.cnso.flinkcdc.service.impl.KaitinggonggaosRelationsServiceImpl;
import com.cnso.flinkcdc.service.impl.KaitinggonggaosServiceImpl;
import com.cnso.flinkcdc.service.impl.KnowledgePropertiesServiceImpl;
import com.cnso.flinkcdc.service.impl.LastIndustryServiceImpl;
import com.cnso.flinkcdc.service.impl.LawsuitsRolerelationsServiceImpl;
import com.cnso.flinkcdc.service.impl.LicenseDrugServiceImpl;
import com.cnso.flinkcdc.service.impl.LicenseInfoServiceImpl;
import com.cnso.flinkcdc.service.impl.LogoutannouncementServiceImpl;
import com.cnso.flinkcdc.service.impl.MakerSpaceServiceImpl;
import com.cnso.flinkcdc.service.impl.ManagementSystemServiceImpl;
import com.cnso.flinkcdc.service.impl.ManufacturingChampionServiceImpl;
import com.cnso.flinkcdc.service.impl.MedicalAdvertisingServiceImpl;
import com.cnso.flinkcdc.service.impl.MedicalInstrumentsBusinessServiceImpl;
import com.cnso.flinkcdc.service.impl.MedicalInstrumentsImportServiceImpl;
import com.cnso.flinkcdc.service.impl.MedicalInstrumentsProduceServiceImpl;
import com.cnso.flinkcdc.service.impl.MedicalInstrumentsRegisteredServiceImpl;
import com.cnso.flinkcdc.service.impl.MedicineExtractServiceImpl;
import com.cnso.flinkcdc.service.impl.MigrationEnterpriseServiceImpl;
import com.cnso.flinkcdc.service.impl.MiningExplorationServiceImpl;
import com.cnso.flinkcdc.service.impl.MiningLicenseServiceImpl;
import com.cnso.flinkcdc.service.impl.MortgagesServiceImpl;
import com.cnso.flinkcdc.service.impl.NameIndexFullServiceImpl;
import com.cnso.flinkcdc.service.impl.NetworkLicenseServiceImpl;
import com.cnso.flinkcdc.service.impl.NewJobsServiceImpl;
import com.cnso.flinkcdc.service.impl.NonCoalMineServiceImpl;
import com.cnso.flinkcdc.service.impl.NoticesRelationsServiceImpl;
import com.cnso.flinkcdc.service.impl.NoticesServiceImpl;
import com.cnso.flinkcdc.service.impl.OrgNoIndexFullServiceImpl;
import com.cnso.flinkcdc.service.impl.OverduetaxsServiceImpl;
import com.cnso.flinkcdc.service.impl.PartnersAllsServiceImpl;
import com.cnso.flinkcdc.service.impl.PatentsInfoServiceImpl;
import com.cnso.flinkcdc.service.impl.PatentsRelationsInfoServiceImpl;
import com.cnso.flinkcdc.service.impl.PayTaxesServiceImpl;
import com.cnso.flinkcdc.service.impl.PesticideProduceEnterpriseServiceImpl;
import com.cnso.flinkcdc.service.impl.PesticideProduceServiceImpl;
import com.cnso.flinkcdc.service.impl.PrintServiceImpl;
import com.cnso.flinkcdc.service.impl.PrivateTechnologyServiceImpl;
import com.cnso.flinkcdc.service.impl.PrivatefundEmprecordServiceImpl;
import com.cnso.flinkcdc.service.impl.PrivatefundManagerServiceImpl;
import com.cnso.flinkcdc.service.impl.PrivatefundMgrdishonestServiceImpl;
import com.cnso.flinkcdc.service.impl.PrivatefundPartnerServiceImpl;
import com.cnso.flinkcdc.service.impl.ProPersonServiceImpl;
import com.cnso.flinkcdc.service.impl.ProcedureCodeServiceImpl;
import com.cnso.flinkcdc.service.impl.ProductSafeLicenseServiceImpl;
import com.cnso.flinkcdc.service.impl.ProjectInfoServiceImpl;
import com.cnso.flinkcdc.service.impl.ProjectMemberServiceImpl;
import com.cnso.flinkcdc.service.impl.ProjectTagServiceImpl;
import com.cnso.flinkcdc.service.impl.RadioCustomsApproveServiceImpl;
import com.cnso.flinkcdc.service.impl.RadioModelApproveServiceImpl;
import com.cnso.flinkcdc.service.impl.RegNoIndexFullServiceImpl;
import com.cnso.flinkcdc.service.impl.ReportDetailsServiceImpl;
import com.cnso.flinkcdc.service.impl.RestrictedConsumerServiceImpl;
import com.cnso.flinkcdc.service.impl.SeriousIllegalServiceImpl;
import com.cnso.flinkcdc.service.impl.ServiceServiceImpl;
import com.cnso.flinkcdc.service.impl.SewagePermitServiceImpl;
import com.cnso.flinkcdc.service.impl.SimpleCancellationAnnouncementServiceImpl;
import com.cnso.flinkcdc.service.impl.SocialSecurityServiceImpl;
import com.cnso.flinkcdc.service.impl.SoftwareServiceImpl;
import com.cnso.flinkcdc.service.impl.SpecialNewLittleGiantServiceImpl;
import com.cnso.flinkcdc.service.impl.SpecializedAndNewEnterpriseServiceImpl;
import com.cnso.flinkcdc.service.impl.SurveyingServiceImpl;
import com.cnso.flinkcdc.service.impl.SzseEidPnamePidServiceImpl;
import com.cnso.flinkcdc.service.impl.SzsePidRelationServiceImpl;
import com.cnso.flinkcdc.service.impl.TechnologicalAllsServiceImpl;
import com.cnso.flinkcdc.service.impl.TechnologicalInnovationDemonstrationServiceImpl;
import com.cnso.flinkcdc.service.impl.TechnologyCenterServiceImpl;
import com.cnso.flinkcdc.service.impl.TechnologyGiantServiceImpl;
import com.cnso.flinkcdc.service.impl.TelecomApprovalServiceImpl;
import com.cnso.flinkcdc.service.impl.TelecomLicenceServiceImpl;
import com.cnso.flinkcdc.service.impl.TelephonesServiceImpl;
import com.cnso.flinkcdc.service.impl.TerminationcaseitemServiceImpl;
import com.cnso.flinkcdc.service.impl.TorchPlanServiceImpl;
import com.cnso.flinkcdc.service.impl.TrademarkInfoServiceImpl;
import com.cnso.flinkcdc.service.impl.TrademarkNoticeServiceImpl;
import com.cnso.flinkcdc.service.impl.TrademarkProductServiceImpl;
import com.cnso.flinkcdc.service.impl.TrademarkRelationServiceImpl;
import com.cnso.flinkcdc.service.impl.TrademarkStepServiceImpl;
import com.cnso.flinkcdc.service.impl.UnicornEnterpriseServiceImpl;
import com.cnso.flinkcdc.service.impl.WebsitesServiceImpl;
import com.cnso.flinkcdc.service.impl.WholeParkRelationServiceImpl;
import com.cnso.flinkcdc.service.impl.WholeParkServiceImpl;
import com.cnso.flinkcdc.service.impl.WildebeestEnterpriseServiceImpl;
import com.cnso.flinkcdc.service.impl.YoungEagleEnterpriseServiceImpl;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create by Zyy 2023-04-24 0024 09:19:05
 */
public class ServiceFactory {
    private final static Logger log = LoggerFactory.getLogger(ServiceFactory.class);

    public static Map<String, BaseService> SERVICE_MAP = new ConcurrentHashMap<>();

    public static BaseService createService(ETableRelation tableRelation){
        BaseService service = null;
        if (null == service){
            //处理任务数据
            switch (tableRelation.getNewTableName()){
                case "t_whole_park":
                    service = new WholeParkServiceImpl();
                    break;
                case "t_whole_park_relation":
                    service = new WholeParkRelationServiceImpl();
                    break;
                case "t_country_code":
                    service = new CountryCodeServiceImpl();
                    break;
                case "t_currency_code":
                    service = new CurrencyCodeServiceImpl();
                    break;
                case "t_econ_kind_code":
                    service = new EconKindCodeServiceImpl();
                    break;
                case "t_industry_code":
                    service = new IndustryCodeServiceImpl();
                    break;
                case "t_job_title_code":
                    service = new JobTitleCodeServiceImpl();
                    break;
                case "t_procedure_code":
                    service = new ProcedureCodeServiceImpl();
                    break;
                case "t_bidding_content_2016":
                    service = new BiddingContent2016ServiceImpl();
                    break;
                case "t_bidding_content_2017":
                    service = new BiddingContent2017ServiceImpl();
                    break;
                case "t_bidding_content_2018":
                    service = new BiddingContent2018ServiceImpl();
                    break;
                case "t_bidding_content_2019":
                    service = new BiddingContent2019ServiceImpl();
                    break;
                case "t_bidding_content_2020":
                    service = new BiddingContent2020ServiceImpl();
                    break;
                case "t_bidding_content_2021":
                    service = new BiddingContent2021ServiceImpl();
                    break;
                case "t_bidding_content_2022":
                    service = new BiddingContent2022ServiceImpl();
                    break;
                case "t_bidding_content_2023":
                    service = new BiddingContent2023ServiceImpl();
                    break;
                case "t_bidding_content_2024":
                    service = new BiddingContent2024ServiceImpl();
                    break;
                case "t_bidding_content_other":
                    service = new BiddingContentOtherServiceImpl();
                    break;
                case "t_bidding_related":
                    service = new BiddingRelatedServiceImpl();
                    break;
                case "t_administrative_punishment":
                    service = new AdministrativePunishmentServiceImpl();
                    break;
                case "t_huanbaochufas":
                    service = new HuanbaochufasServiceImpl();
                    break;
                case "t_executions":
                    service = new ExecutionsServiceImpl();
                    break;
                case "t_restricted_consumer":
                    service = new RestrictedConsumerServiceImpl();
                    break;
                case "t_terminationcaseitem":
                    service = new TerminationcaseitemServiceImpl();
                    break;
                case "t_address":
                    service = new AddressServiceImpl();
                    break;
                case "t_best_brief":
                    service = new BestBriefServiceImpl();
                    break;
                case "t_branches":
                    service = new BranchesServiceImpl();
                    break;
                case "t_change_records":
                    service = new ChangeRecordsServiceImpl();
                    break;
                case "t_emails":
                    service = new EmailsServiceImpl();
                    break;
                case "t_enterprise":
                    service = new EnterpriseServiceImpl();
                    break;
                case "t_history_names":
                    service = new HistoryNamesServiceImpl();
                    break;
                case "t_history_oper_names":
                    service = new HistoryOperNamesServiceImpl();
                    break;
                case "t_history_regist_capis":
                    service = new HistoryRegistCapisServiceImpl();
                    break;
                case "t_investments":
                    service = new InvestmentsServiceImpl();
                    break;
                case "t_last_industry":
                    service = new LastIndustryServiceImpl();
                    break;
                case "t_partners_alls":
                    service = new PartnersAllsServiceImpl();
                    break;
                case "t_telephones":
                    service = new TelephonesServiceImpl();
                    break;
                case "t_websites":
                    service = new WebsitesServiceImpl();
                    break;
                case "t_report_details":
                    service = new ReportDetailsServiceImpl();
                    break;
                case "t_social_security":
                    service = new SocialSecurityServiceImpl();
                    break;
                case "t_abnormal":
                    service = new AbnormalServiceImpl();
                    break;
                case "t_checkups":
                    service = new CheckupsServiceImpl();
                    break;
                case "t_clear_account":
                    service = new ClearAccountServiceImpl();
                    break;
                case "t_double_checkups":
                    service = new DoubleCheckupsServiceImpl();
                    break;
                case "t_equityquality":
                    service = new EquityqualityServiceImpl();
                    break;
                case "t_judicial_freezes":
                    service = new JudicialFreezesServiceImpl();
                    break;
                case "t_knowledge_properties":
                    service = new KnowledgePropertiesServiceImpl();
                    break;
                case "t_license_info":
                    service = new LicenseInfoServiceImpl();
                    break;
                case "t_logoutannouncement":
                    service = new LogoutannouncementServiceImpl();
                    break;
                case "t_mortgages":
                    service = new MortgagesServiceImpl();
                    break;
                case "t_serious_illegal":
                    service = new SeriousIllegalServiceImpl();
                    break;
                case "t_simple_cancellation_announcement":
                    service = new SimpleCancellationAnnouncementServiceImpl();
                    break;
                case "t_privatefund_emprecord":
                    service = new PrivatefundEmprecordServiceImpl();
                    break;
                case "t_privatefund_manager":
                    service = new PrivatefundManagerServiceImpl();
                    break;
                case "t_privatefund_mgrdishonest":
                    service = new PrivatefundMgrdishonestServiceImpl();
                    break;
                case "t_privatefund_partner":
                    service = new PrivatefundPartnerServiceImpl();
                    break;
                case "t_company_finance":
                    service = new CompanyFinanceServiceImpl();
                    break;
                case "t_company_products":
                    service = new CompanyProductsServiceImpl();
                    break;
                case "t_company_similars":
                    service = new CompanySimilarsServiceImpl();
                    break;
                case "t_invest_info":
                    service = new InvestInfoServiceImpl();
                    break;
                case "t_investment_events":
                    service = new InvestmentEventsServiceImpl();
                    break;
                case "t_project_info":
                    service = new ProjectInfoServiceImpl();
                    break;
                case "t_project_member":
                    service = new ProjectMemberServiceImpl();
                    break;
                case "t_project_tag":
                    service = new ProjectTagServiceImpl();
                    break;
                case "t_copyrights_relations":
                    service = new CopyrightsRelationsServiceImpl();
                    break;
                case "t_patents_info":
                    service = new PatentsInfoServiceImpl();
                    break;
                case "t_auctions_relations":
                    service = new AuctionsRelationsServiceImpl();
                    break;
                case "t_cases":
                    service = new CasesServiceImpl();
                    break;
                case "t_cases_relations":
                    service = new CasesRelationsServiceImpl();
                    break;
                case "t_kaitinggonggaos":
                    service = new KaitinggonggaosServiceImpl();
                    break;
                case "t_kaitinggonggaos_relations":
                    service = new KaitinggonggaosRelationsServiceImpl();
                    break;
                case "t_notices":
                    service = new NoticesServiceImpl();
                    break;
                case "t_notices_relations":
                    service = new NoticesRelationsServiceImpl();
                    break;
                case "t_lawsuits_rolerelations":
                    service = new LawsuitsRolerelationsServiceImpl();
                    break;
                case "t_creditimportexport_data":
                    service = new CreditimportexportDataServiceImpl();
                    break;
                case "t_goudi_information":
                    service = new GoudiInformationServiceImpl();
                    break;
                case "t_new_jobs":
                    service = new NewJobsServiceImpl();
                    break;
                case "t_migration_enterprise":
                    service = new MigrationEnterpriseServiceImpl();
                    break;
                case "t_abnormal_enterprises":
                    service = new AbnormalEnterprisesServiceImpl();
                    break;
                case "t_general_taxpayer":
                    service = new GeneralTaxpayerServiceImpl();
                    break;
                case "t_huge_tax_punishment":
                    service = new HugeTaxPunishmentServiceImpl();
                    break;
                case "t_overduetaxs":
                    service = new OverduetaxsServiceImpl();
                    break;
                case "t_pay_taxes":
                    service = new PayTaxesServiceImpl();
                    break;
                case "t_trademark_info":
                    service = new TrademarkInfoServiceImpl();
                    break;
                case "t_trademark_product":
                    service = new TrademarkProductServiceImpl();
                    break;
                case "t_trademark_step":
                    service = new TrademarkStepServiceImpl();
                    break;
                case "t_trademark_notice":
                    service = new TrademarkNoticeServiceImpl();
                    break;
                case "t_trademark_relation":
                    service = new TrademarkRelationServiceImpl();
                    break;
                case "t_article_content":
                    service = new ArticleContentServiceImpl();
                    break;
                case "t_article_enterprise":
                    service = new ArticleEnterpriseServiceImpl();
                    break;
                case "t_article_enterprise_tags":
                    service = new ArticleEnterpriseTagsServiceImpl();
                    break;
                case "t_article_mining":
                    service = new ArticleMiningServiceImpl();
                    break;
                case "t_credit_no_index_full":
                    service = new CreditNoIndexFullServiceImpl();
                    break;
                case "t_name_index_full":
                    service = new NameIndexFullServiceImpl();
                    break;
                case "t_org_no_index_full":
                    service = new OrgNoIndexFullServiceImpl();
                    break;
                case "t_reg_no_index_full":
                    service = new RegNoIndexFullServiceImpl();
                    break;
                case "t_advanced_technology_service":
                    service = new AdvancedTechnologyServiceServiceImpl();
                    break;
                case "t_building":
                    service = new BuildingServiceImpl();
                    break;
                case "t_building_engineering":
                    service = new BuildingEngineeringServiceImpl();
                    break;
                case "t_business_concession":
                    service = new BusinessConcessionServiceImpl();
                    break;
                case "t_business_incubator":
                    service = new BusinessIncubatorServiceImpl();
                    break;
                case "t_ccc":
                    service = new CccServiceImpl();
                    break;
                case "t_certificate_main":
                    service = new CertificateMainServiceImpl();
                    break;
                case "t_chinese_medicine_record":
                    service = new ChineseMedicineRecordServiceImpl();
                    break;
                case "t_civilaviation_business_license":
                    service = new ChineseMedicineRecordServiceImpl();
                    break;
                case "t_computer_project_manager":
                    service = new ComputerProjectManagerServiceImpl();
                    break;
                case "t_cosmetic_nonspecial":
                    service = new CosmeticNonspecialServiceImpl();
                    break;
                case "t_cosmetic_produce_license":
                    service = new CosmeticProduceLicenseServiceImpl();
                    break;
                case "t_cosmetic_special":
                    service = new CosmeticSpecialServiceImpl();
                    break;
                case "t_drug_business":
                    service = new DrugBusinessServiceImpl();
                    break;
                case "t_drug_permit_cn":
                    service = new DrugPermitCnServiceImpl();
                    break;
                case "t_drug_produce":
                    service = new DrugProduceServiceImpl();
                    break;
                case "t_drug_record":
                    service = new DrugRecordServiceImpl();
                    break;
                case "t_electronic_certification_agency":
                    service = new ElectronicCertificationAgencyServiceImpl();
                    break;
                case "t_estate_qualification":
                    service = new EstateEualificationServiceImpl();
                    break;
                case "t_financial":
                    service = new FinancialServiceImpl();
                    break;
                case "t_firefighting_system":
                    service = new FirefightingSystemServiceImpl();
                    break;
                case "t_food_additives":
                    service = new FoodAdditivesServiceImpl();
                    break;
                case "t_food_license":
                    service = new FoodLicenseServiceImpl();
                    break;
                case "t_food_product_ent":
                    service = new FoodProductEntServiceImpl();
                    break;
                case "t_food_product_license":
                    service = new FoodProductLicenseServiceImpl();
                    break;
                case "t_forestry_permit":
                    service = new ForestryPermitServiceImpl();
                    break;
                case "t_game":
                    service = new GameServiceImpl();
                    break;
                case "t_hazardous_chemicals_licence":
                    service = new HazardousChemicalsLicenceServiceImpl();
                    break;
                case "t_hightech_enterprises":
                    service = new HightechEnterprisesServiceImpl();
                    break;
                case "t_hygiene_product":
                    service = new HygieneProductServiceImpl();
                    break;
                case "t_industry_produce":
                    service = new IndustryProduceServiceImpl();
                    break;
                case "t_industry_volunteer":
                    service = new IndustryVolunteerServiceImpl();
                    break;
                case "t_innovative_enterprises":
                    service = new InnovativeEnterprisesServiceImpl();
                    break;
                case "t_internet_drug":
                    service = new InternetDrugServiceImpl();
                    break;
                case "t_invisible_champion":
                    service = new InvisibleChampionServiceImpl();
                    break;
                case "t_it_safe":
                    service = new ItSafeServiceImpl();
                    break;
                case "t_license_drug":
                    service = new LicenseDrugServiceImpl();
                    break;
                case "t_maker_space":
                    service = new MakerSpaceServiceImpl();
                    break;
                case "t_management_system":
                    service = new ManagementSystemServiceImpl();
                    break;
                case "t_manufacturing_champion":
                    service = new ManufacturingChampionServiceImpl();
                    break;
                case "t_medical_advertising":
                    service = new MedicalAdvertisingServiceImpl();
                    break;
                case "t_medical_instruments_business":
                    service = new MedicalInstrumentsBusinessServiceImpl();
                    break;
                case "t_medical_instruments_import":
                    service = new MedicalInstrumentsImportServiceImpl();
                    break;
                case "t_medical_instruments_produce":
                    service = new MedicalInstrumentsProduceServiceImpl();
                    break;
                case "t_medical_instruments_registered":
                    service = new MedicalInstrumentsRegisteredServiceImpl();
                    break;
                case "t_medicine_extract":
                    service = new MedicineExtractServiceImpl();
                    break;
                case "t_mining_exploration":
                    service = new MiningExplorationServiceImpl();
                    break;
                case "t_mining_license":
                    service = new MiningLicenseServiceImpl();
                    break;
                case "t_network_license":
                    service = new NetworkLicenseServiceImpl();
                    break;
                case "t_non_coal_mine":
                    service = new NonCoalMineServiceImpl();
                    break;
                case "t_pesticide_produce":
                    service = new PesticideProduceServiceImpl();
                    break;
                case "t_pesticide_produce_enterprise":
                    service = new PesticideProduceEnterpriseServiceImpl();
                    break;
                case "t_print":
                    service = new PrintServiceImpl();
                    break;
                case "t_private_technology":
                    service = new PrivateTechnologyServiceImpl();
                    break;
                case "t_pro_person":
                    service = new ProPersonServiceImpl();
                    break;
                case "t_product_safe_license":
                    service = new ProductSafeLicenseServiceImpl();
                    break;
                case "t_radio_customs_approve":
                    service = new RadioCustomsApproveServiceImpl();
                    break;
                case "t_radio_model_approve":
                    service = new RadioModelApproveServiceImpl();
                    break;
                case "t_service":
                    service = new ServiceServiceImpl();
                    break;
                case "t_sewage_permit":
                    service = new SewagePermitServiceImpl();
                    break;
                case "t_software":
                    service = new SoftwareServiceImpl();
                    break;
                case "t_special_new_little_giant":
                    service = new SpecialNewLittleGiantServiceImpl();
                    break;
                case "t_specialized_and_new_enterprise":
                    service = new SpecializedAndNewEnterpriseServiceImpl();
                    break;
                case "t_surveying":
                    service = new SurveyingServiceImpl();
                    break;
                case "t_technological_alls":
                    service = new TechnologicalAllsServiceImpl();
                    break;
                case "t_technological_innovation_demonstration":
                    service = new TechnologicalInnovationDemonstrationServiceImpl();
                    break;
                case "t_technology_center":
                    service = new TechnologyCenterServiceImpl();
                    break;
                case "t_technology_giant":
                    service = new TechnologyGiantServiceImpl();
                    break;
                case "t_telecom_approval":
                    service = new TelecomApprovalServiceImpl();
                    break;
                case "t_telecom_licence":
                    service = new TelecomLicenceServiceImpl();
                    break;
                case "t_torch_plan":
                    service = new TorchPlanServiceImpl();
                    break;
                case "t_unicorn_enterprise":
                    service = new UnicornEnterpriseServiceImpl();
                    break;
                case "t_wildebeest_enterprise":
                    service = new WildebeestEnterpriseServiceImpl();
                    break;
                case "t_young_eagle_enterprise":
                    service = new YoungEagleEnterpriseServiceImpl();
                    break;
                case "szse_eid_pname_pid":
                    service = new SzseEidPnamePidServiceImpl();
                    break;
                case "szse_pid_relation":
                    service = new SzsePidRelationServiceImpl();
                    break;
                case "t_executedpersons":
                    service = new ExecutedpersonsServiceImpl();
                    break;
                case "t_admin_division_code":
                    service = new AdminDivisionCodeServiceImpl();
                    break;
                case "t_admin_division_code_change_history":
                    service = new AdminDivisionCodeChangeHistoryServiceImpl();
                    break;
                case "t_best_stock_info":
                    service = new BestStockInfoServiceImpl();
                    break;
                case "t_employees_alls":
                    service = new EmployeesAllsServiceImpl();
                    break;
                case "t_domains_alls":
                    service = new DomainsAllsServiceImpl();
                    break;
                case "t_patents_relations_info":
                    service = new PatentsRelationsInfoServiceImpl();
                    break;
                default:
                    log.error("[任务类型异常] 未找到对应的任务类型，任务信息：{}", JSON.toJSONString(tableRelation));
            }
        }
        return service;
    }
}
