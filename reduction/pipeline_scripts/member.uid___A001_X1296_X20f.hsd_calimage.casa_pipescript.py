from recipes.almahelpers import fixsyscaltimes # SACM/JAO - Fixes
__rethrow_casa_exceptions = True
context = h_init()
context.set_state('ProjectSummary', 'proposal_code', '2017.1.01355.L')
context.set_state('ProjectSummary', 'piname', 'unknown')
context.set_state('ProjectSummary', 'proposal_title', 'unknown')
context.set_state('ProjectStructure', 'ous_part_id', 'X2067113871')
context.set_state('ProjectStructure', 'ous_title', 'Undefined')
context.set_state('ProjectStructure', 'ppr_file', '/opt/dared/opt/c5r1/mnt/dataproc/2017.1.01355.L_2018_01_18T14_46_09.503/SOUS_uid___A001_X1296_X207/GOUS_uid___A001_X1296_X208/MOUS_uid___A001_X1296_X20f/working/PPR_uid___A001_X1296_X210.xml')
context.set_state('ProjectStructure', 'ps_entity_id', 'uid://A001/X1220/Xddd')
context.set_state('ProjectStructure', 'recipe_name', 'hsd_calimage')
context.set_state('ProjectStructure', 'ous_entity_id', 'uid://A001/X1220/Xdd9')
context.set_state('ProjectStructure', 'ousstatus_entity_id', 'uid://A001/X1296/X20f')
try:
    hsd_importdata(vis=['uid___A002_Xc9020b_X7c2e', 'uid___A002_Xc91189_X2379'], session=['session_1', 'session_2'])
    hsd_flagdata(pipelinemode="automatic")
    h_tsyscal(pipelinemode="automatic")
    hsd_tsysflag(pipelinemode="automatic")
    hsd_skycal(pipelinemode="automatic")
    hsd_k2jycal(pipelinemode="automatic")
    hsd_applycal(pipelinemode="automatic")
    hsd_baseline(pipelinemode="automatic")
    hsd_blflag(pipelinemode="automatic")
    hsd_baseline(pipelinemode="automatic")
    hsd_blflag(pipelinemode="automatic")
    hsd_imaging(pipelinemode="automatic")
finally:
    h_save()