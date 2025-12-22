
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


default_fig_config = {
        'displaylogo':False, 
        'modeBarButtonsToRemove': [
        'pan2d','select2d','lasso2d',
        'resetScale2d','zoomOut2d', 'zoomIn2d',
        'zoom2d', 'toggleSpikelines', 'hoverClosest3d'
        ]
    }

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
    # graph_title=f'I Chart: {vc.vc_name} of {part} @ {equipment}'
    # context['graph_title'] = graph_title
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

