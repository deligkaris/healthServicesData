'''The chain an analysis runs, get_cms_data to the transfers, step by step: tests/cms/test_end_to_end.py runs it on the
invented files, tests/data/test_regression.py on the real sample. Each step is written to output and read back as the
analysis scripts do; with no output nothing is written and nothing runs, the steps only build their plans.'''
import cms.base as baseF
import cms.revenue as revenueF
import cms.stays as staysF
import cms.transfers as transfersF
import cms.utilities as cmsUtilitiesF
import utilities as utilitiesF
from cms.transfers import TransferType

def kept(df, output, name):
    return df if output is None else utilitiesF.write_and_read_parquet(df, f"{output}/{name}")

def get_cms_data(spark, pathCMS, years, externalData, output):
    dataframes = cmsUtilitiesF.get_cms_data(pathCMS, *years, spark, externalData)
    return {k: kept(df, output, k) for k, df in dataframes.items()}

def get_ip_stays(cmsData, output, dropUnused=False):
    '''The ip stays with the stroke columns and their source and destination; dropUnused: then stays.drop_unused_columns,
    as the analysis scripts do before get_transfers.'''
    ip = baseF.add_claim_stroke_info(baseF.add_ishStroke(cmsData["ipBase"]), inpatient=True)
    stays = staysF.get_stays(ip, revenueF.get_revenue_info(cmsData["ipRevenue"]), claimType="ip")
    stays = staysF.add_source_and_destination_info(stays, cmsData)
    return kept(staysF.drop_unused_columns(stays) if dropUnused else stays, output, "ipStays")

def get_op_stays(cmsData, output, dropUnused=False):
    op = baseF.add_tpa(cmsData["opBase"], inpatient=False)
    stays = staysF.get_stays(op, revenueF.get_revenue_info(cmsData["opRevenue"]), claimType="op")
    return kept(staysF.drop_unused_columns(stays) if dropUnused else stays, output, "opStays")

def get_ip_to_ip(cmsData, ipStays, lastObservableDay, output):
    '''The IP to IP transfers with the stroke columns, the comorbidities and the days at home.'''
    transfers = transfersF.get_transfers(fromStaysDf=ipStays, toStaysDf=ipStays, transferType=TransferType.inpatientToInpatient)
    transfers = transfersF.add_stroke_info(transfers)
    transfers = kept(transfers, output, "ipToIp")
    lookups = {k: kept(df, output, f"lookups/{k}") for k, df in transfersF.get_lookups(transfers, cmsData).items()}
    transfers = transfersF.add_comorbidity_and_days_at_home_info(transfers, lookups, lastObservableDay)
    return kept(transfers, output, "ipToIpFinal")

def get_ed_to_ip(opStays, ipStays, output):
    transfers = transfersF.get_transfers(fromStaysDf=opStays, toStaysDf=ipStays, transferType=TransferType.edToInpatient)
    return kept(transfers, output, "edToIp")

def get_chain(spark, pathCMS, years, externalData, lastObservableDay, output, dropUnused=False):
    '''Every output of the chain: the get_cms_data parts, ipStays, opStays, ipToIp and edToIp.'''
    run = get_cms_data(spark, pathCMS, years, externalData, output)
    return {**run, **get_stays_and_transfers(run, lastObservableDay, output, dropUnused)}

def get_stays_and_transfers(cmsData, lastObservableDay, output, dropUnused=False):
    (ip, op) = (get_ip_stays(cmsData, output, dropUnused), get_op_stays(cmsData, output, dropUnused))
    return {"ipStays": ip, "opStays": op, "ipToIp": get_ip_to_ip(cmsData, ip, lastObservableDay, output),
            "edToIp": get_ed_to_ip(op, ip, output)}
