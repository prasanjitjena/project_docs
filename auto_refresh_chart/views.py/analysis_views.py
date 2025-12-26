from django.shortcuts import render
from app1.models import MapDefectiveSPCScheme, RecordDefectiveData, MapDefectSPCScheme, MapSPCScheme, RecordDefectData, MapPartVariableCharacteristics,MultiStageScheme,MultiStageSchemeItem,ConfVariableCharacterisitics
from app1.models import RecordAlerts, RecordImrIndividualData, RecordSPCSubgroupData, ConfEquipment , ConfPart,MapDefectChoiceKey,MapDefectTextKey,RecordChoiceDefect,RecordTextDefect,MapDefectiveChoiceKey,MapDefectiveTextKey,RecordChoiceDefective,RecordTextDefective,ConfCause,RecordCauseDefective
from app1.forms import ImrAnalysisForm, SpcAnalysisForm, ProcessCapabilityForm, CapabilityCompareVarForm,ProcessCapabilitySubgroupForm,ImrAnalysisGroupBulk1Form
from app1 import logics, businessLogic
import pandas as pd
import scipy
import math
from django.contrib.auth.decorators import login_required
# from django.http import HttpResponse
from django.urls import reverse
import plotly.graph_objects as go
from plotly.offline import plot
from app1.forms import DataEntryDefectiveForm, DataEntryDefectForm, DataEntryIndividualImrForm, PChartAnalysisForm, UChartAnalysisForm,PChartAnalysisRefreshForm
import numpy as np
# from zstat import zs_control_charts
from zstatq import zs_control_charts
from zstatq.zs_hypothesis_test import optimal_lambda_boxcox, normality_test
from app1.logics import get_individual_setfromDB_dict, i_mr, get_subgroup_setfromDB_dict, get_estimated_process_sd_using_r_bar_method
from app1.logics import get_subgroup_mean_control_lines_dict, get_subgroup_range_control_lines_dict, get_individual_setfromDB_dict_1, get_subgroup_setfromDB_dict_1
from app1 import businessLogic
from django.core.mail import send_mail
from datetime import datetime, timedelta, date
# import datetime
from django.conf import settings
import pytz
from plotly.subplots import make_subplots
from zstatq.zs_hypothesis_test import normality_test
import plotly.express as px
from django.contrib.postgres.aggregates import ArrayAgg
from zstatq.zs_process_capability import capability_sixpack_betweeen_within
import plotly.io as pio
import base64
from app1.views1.pdf_views import generate_pdf
from zstatq.zs_control_charts import cumsum_chart,ewma_chart
from django.templatetags.static import static
from intelliqs.context_processors import dynamic_labels


default_fig_config = {
        'displaylogo':False, 
        'modeBarButtonsToRemove': [
        'pan2d','select2d','lasso2d',
        'resetScale2d','zoomOut2d', 'zoomIn2d',
        'zoom2d', 'toggleSpikelines', 'hoverClosest3d'
        ]
    }

def related_schemes(part, equipment, link_type='Data Entry'):
    labels = dynamic_labels()["LABELS"]
    output_dict={}
    group_data_entry_array = []
    related_variable_schemes = MapSPCScheme.objects.filter(part_vc_id__part_id=part, equipment_id=equipment)
    related_variable_schemes_df = pd.DataFrame(related_variable_schemes.values('id','part_vc_id__vc_id__vc_name','store_as_subgroup','part_vc_id__sequence_priority'))
    # Sorting according to sequence_priority and id
    if len(related_variable_schemes_df.index)>0:
        related_variable_schemes_df = related_variable_schemes_df.sort_values(
            by=["part_vc_id__sequence_priority", "id"], ascending=[True, True]
        ).reset_index(drop=True)
    imr_count = 0
    xbarr_count =0
    links = []
    all_group_rev = reverse('app1:dataentry_all_groups', args=[equipment.pk, part.pk, "new"])
    group_data_entry_array.append(f'<a href="{all_group_rev}">All Groups</a>')

    for i in related_variable_schemes_df.index:
        if related_variable_schemes_df.loc[i,'store_as_subgroup']: 
            text = related_variable_schemes_df.loc[i,'part_vc_id__vc_id__vc_name'] + ' (Xbar-R)'
            hl = reverse('app1:dataentry_variablesubgroup', args=[related_variable_schemes_df.loc[i,'id'],])
            hl_eqp = reverse('app1:data_entry_subgroup_all_eqp', args=[related_variable_schemes_df.loc[i,'id'],])
            xbarr_count+=1
        else:
            text = related_variable_schemes_df.loc[i,'part_vc_id__vc_id__vc_name'] + ' (I-MR)'
            hl = reverse('app1:dataentry_individual', args=[related_variable_schemes_df.loc[i,'id'],])
            hl_eqp = reverse('app1:dataentry_imr_all_eqp', args=[related_variable_schemes_df.loc[i,'id'],])
            imr_count+=1
        col_name = f"Single Data Entry - {labels['variable']}"
        related_variable_schemes_df.loc[i,col_name] = f'<a href="{hl}">{text}</a>'
        related_variable_schemes_df.loc[i,'Multi-Eqp Data Entry'] = f'<a href="{hl_eqp}">{text}</a>'
    if len(related_variable_schemes_df.index) > 0:
        related_variable_schemes_df['part_vc_id__sequence_priority'] = related_variable_schemes_df['part_vc_id__sequence_priority'].fillna(0).astype(int)
        output_dict['related_variable_schemes_df_html'] = related_variable_schemes_df[[col_name]].to_html(index=False, render_links=True, escape=False, classes = "table table-bordered")
        output_dict['multi_eqp_variable_schemes_df_html'] = related_variable_schemes_df[['Multi-Eqp Data Entry']].to_html(index=False, render_links=True, escape=False, classes = "table table-bordered")
    if imr_count >0:
        # hl = reverse('app1:dataentry_imr_all_variables', args=[equipment.pk, part.pk])
        hl = reverse('app1:dataentry_imr_all_variables', args=[equipment.pk, part.pk, "new"])
        h2 = reverse('app1:dataentry_imr_group_bulk', args=[equipment.pk, part.pk]) 
        h3 = reverse('app1:dataentry_imr_group_bulk_1', args=[equipment.pk, part.pk]) 

        group_data_entry_array.append(f'<a href="{hl}">All {labels["variable"]} (I-MR)</a>')
        group_data_entry_array.append(f'<a href="{h2}">Group Bulk (I-MR)</a>')
        group_data_entry_array.append(f'<a href="{h3}">Group Bulk 2 (I-MR)</a>')

    if xbarr_count >0:
        hl = reverse('app1:dataentry_subgroup_all_variables', args=[equipment.pk, part.pk])
        group_data_entry_array.append(f'<a href="{hl}">All {labels["variable"]} (Xbar-R)</a>')
    
    related_defective_schemes = MapDefectiveSPCScheme.objects.filter(part=part, equipment=equipment)
    related_defective_schemes_df = pd.DataFrame(related_defective_schemes.values('id','defective_test__name','defective_test__sequence_priority'))

    if len(related_defective_schemes_df.index) > 0:
        related_defective_schemes_df = related_defective_schemes_df.sort_values(
            by=["defective_test__sequence_priority", "id"], ascending=[True, True]
        ).reset_index(drop=True)

    for i in related_defective_schemes_df.index:
        text = related_defective_schemes_df.loc[i,'defective_test__name']
        hl = reverse('app1:dataentry_defective', args=[related_defective_schemes_df.loc[i,'id'],])
        related_defective_schemes_df.loc[i,'Single Data Entry - Defective (p)'] = f'<a href="{hl}">{text}</a>'

    if len(related_defective_schemes_df.index) > 0:
        related_defective_schemes_df = related_defective_schemes_df.sort_values(
            by=["defective_test__sequence_priority", "id"], ascending=[True, True]
        ).reset_index(drop=True)
        related_defective_schemes_df['defective_test__sequence_priority'] = related_defective_schemes_df['defective_test__sequence_priority'].fillna(0).astype(int)
        output_dict['related_defective_schemes_df_html'] = related_defective_schemes_df[['Single Data Entry - Defective (p)']].to_html(index=False, render_links=True, escape=False, classes = "table table-bordered")
        hl = reverse('app1:dataentry_defective_all', args=[equipment.pk, part.pk])
        # group_data_entry_array.append(f'<a href="{hl}">All defectives (P Chart)</a>')
        group_data_entry_array.append(f'<a href="{hl}">All defectives</a>')

    related_defect_schemes = MapDefectSPCScheme.objects.filter(part=part, equipment=equipment)
    related_defect_schemes_df = pd.DataFrame(related_defect_schemes.values('id','defect_test__name','defect_test__sequence_priority'))
    
    if len(related_defect_schemes_df.index) > 0:
        related_defect_schemes_df = related_defect_schemes_df.sort_values(
            by=["defect_test__sequence_priority", "id"], ascending=[True, True]
        ).reset_index(drop=True)

    for i in related_defect_schemes_df.index:
        text = related_defect_schemes_df.loc[i,'defect_test__name']
        hl = reverse('app1:dataentry_defect', args=[related_defect_schemes_df.loc[i,'id'],])
        related_defect_schemes_df.loc[i,'Single Data Entry - Defect (u)'] = f'<a href="{hl}">{text}</a>'

    if len(related_defect_schemes_df.index) > 0:
        related_defect_schemes_df['defect_test__sequence_priority'] = related_defect_schemes_df['defect_test__sequence_priority'].fillna(0).astype(int)
        output_dict['related_defect_schemes_df_html'] = related_defect_schemes_df[['Single Data Entry - Defect (u)']].to_html(index=False, render_links=True, escape=False, classes = "table table-bordered")
        hl = reverse('app1:dataentry_defect_all', args=[equipment.pk, part.pk])
        # group_data_entry_array.append(f'<a href="{hl}">All defects (U Chart)</a>')
        group_data_entry_array.append(f'<a href="{hl}">All defects</a>')

    if len(group_data_entry_array) >0:
        output_dict['group_data_entry_df_html'] = pd.DataFrame({"Multi-Test Data Entry": group_data_entry_array}).to_html(index=False, render_links=True, escape=False, classes = "table table-bordered")
    
    # Add link_type and prepare unified DataFrame
    if len(related_variable_schemes_df.index) > 0:
        related_variable_schemes_df = related_variable_schemes_df.assign(
            link_type=lambda df: df['store_as_subgroup'].map({True: 'variablesubgroup', False: 'individual'}),
            sequence_priority=related_variable_schemes_df['part_vc_id__sequence_priority']
        )[['id', 'link_type', 'sequence_priority']]

    if len(related_defective_schemes_df.index) > 0:
        related_defective_schemes_df = related_defective_schemes_df.assign(
            link_type='defective',
            sequence_priority=related_defective_schemes_df['defective_test__sequence_priority']
        )[['id', 'link_type', 'sequence_priority']]

    if len(related_defect_schemes_df.index) > 0:
        related_defect_schemes_df = related_defect_schemes_df.assign(
            link_type='defect',
            sequence_priority=related_defect_schemes_df['defect_test__sequence_priority']
        )[['id', 'link_type', 'sequence_priority']]

    # Merge all DataFrames into one
    merged_df = pd.concat([related_variable_schemes_df, related_defective_schemes_df, related_defect_schemes_df])
    # Sort the unified DataFrame by sequence_priority and id
    merged_df = merged_df.sort_values(by=['sequence_priority', 'id'])
    links = []
    analysis_links = []
    # Generate links based on the merged DataFrame
    if link_type == 'Analysis':
        for _, row in merged_df.iterrows():
            sid = row['id']
            if row['link_type'] == 'individual':
                analysis_links.append(reverse('app1:imranalysis', args=[sid]))
            elif row['link_type'] == 'variablesubgroup':
                analysis_links.append(reverse('app1:spcanalysis', args=[sid]))
            elif row['link_type'] == 'defective':
                analysis_links.append(reverse('app1:pchart_analysis', args=[sid]))
            elif row['link_type'] == 'defect':
                analysis_links.append(reverse('app1:uchart_analysis', args=[sid]))
            # else:
            #     analysis_links.append(reverse(f'app1:dataentry_{row["link_type"]}', args=[row['id']]))
        output_dict['links'] = analysis_links        
    else:
        for _, row in merged_df.iterrows():
            # Construct the URL using reverse() for each row
            if row['link_type'] == 'variablesubgroup':
                links.append(reverse('app1:dataentry_variablesubgroup', args=[row['id']]))
            elif row['link_type'] == 'individual':
                links.append(reverse('app1:dataentry_individual', args=[row['id']]))
            else:
                links.append(reverse(f'app1:dataentry_{row["link_type"]}', args=[row['id']]))

        output_dict['links'] = links
    return output_dict


@login_required(login_url='admin:login')
def imr_chart_refresh(request, scheme_id):
    scheme = MapSPCScheme.objects.get(pk=scheme_id)
    context = {'title': "I-MR Chart Refresh",'scheme': scheme, 'scheme_id': scheme_id,}
    part = scheme.part_vc_id.part_id
    vc = scheme.part_vc_id.vc_id
    uom = scheme.part_vc_id.unit
    equipment = scheme.equipment_id
    work_unit_timezone = equipment.work_unit_id.timezone
    template_filter = scheme.part_vc_id
    historical_mean=scheme.historical_mu,
    historical_s = scheme.historical_sigma,
    i_graph_title = f'I Chart: {vc.vc_name} of {part.part_no} @ {equipment.equipment_name}'
    mr_graph_title = f'MR Chart: {vc.vc_name} of {part.part_no} @ {equipment.equipment_name}'
    context['add_icon_url']  = static('admin/img/icon-addlink.svg')
    context['i_graph_title'] = i_graph_title
    context['mr_graph_title'] = mr_graph_title
    h_cusum = template_filter.alert_template.h_cusum
    k_cusum = template_filter.alert_template.k_cusum
    est_std_dev_method_1 = template_filter.alert_template.est_std_dev_method_1
    est_std_dev_method_2 = template_filter.alert_template.est_std_dev_method_2
    moving_range_length = template_filter.alert_template.moving_range_length
    use_unbiasing_constant = template_filter.alert_template.use_unbiasing_constant
    cumsum_title = f'CUSUM Chart: {vc.vc_name} of {part.part_no} @ {equipment.equipment_name}'
    reset_after_each_signal = template_filter.alert_template.reset_after_each_signal
    ewma_weight = template_filter.alert_template.ewma_weight
    k_r1 = template_filter.alert_template.x_rule_1
    rule_1_b = template_filter.alert_template.x_rule_1_b
    hist_mean = scheme.historical_mu
    hist_sigma = scheme.historical_sigma
    usl = scheme.part_vc_id.upper_spec_limit
    lsl= scheme.part_vc_id.lower_spec_limit
    time_weigh_chrt_analysis = scheme.time_weighted_chart
    time_weigh_chrt_title = f'{time_weigh_chrt_analysis} Chart: {vc.vc_name} of {part.part_no} @ {equipment.equipment_name}'
    if uom is not None:
        yaxis_title = f'{vc.vc_name}({uom.uom_unique})'
    else:
        yaxis_title = vc.vc_name
    context['yaxis_title'] = yaxis_title
    chart_option = scheme.INDIVIDUAL_CHART_OPTIONS
    if historical_mean is not None:
        context['historical_mean'] = True
    else:
        context['historical_mean'] = False
    if historical_s is not None:
        context['historical_s'] = True
    else:
        context['historical_s'] = False

    context['work_unit_timezone'] = work_unit_timezone
    all_causes = list(ConfCause.objects.values_list("name",flat=True))
    context['all_causes'] = all_causes
    form = ImrAnalysisForm()
    context['form'] = form
    context.update(related_schemes(part, equipment))
    return render(request, 'app1/api_entry/refresh_imr_api.html', context)


@login_required(login_url='admin:login')
def realtime_pchart_refresh(request, scheme_id):
    scheme = MapDefectiveSPCScheme.objects.get(pk=scheme_id)
    context = {
        "title":" P Chart Refresh (for Defectives)",
        'scheme': scheme, 
        'scheme_id': scheme_id,
    }
    part = scheme.part
    defective_test = scheme.defective_test
    equipment = scheme.equipment
    work_unit_timezone = equipment.work_unit_id.timezone
    chart_option = scheme.chart_options
    historical_p = scheme.historical_defective_rate 
    chart_option = scheme.chart_options 
    context['chart_option'] = chart_option
    if historical_p is not None:
        context['historical_p'] = True
    else:
        context['historical_p'] = False
    context['work_unit_timezone'] = work_unit_timezone
    graph_title=f'{chart_option} Chart: {defective_test.name} of {part} @ {equipment}'
    context['graph_title'] = graph_title
    form = PChartAnalysisForm()
    context['form'] = form
    context.update(related_schemes(part, equipment))
    return render(request, 'app1/api_entry/realtime_pchart1.html', context)

