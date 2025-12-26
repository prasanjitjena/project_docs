from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from app1.models import RecordImrIndividualData,RecordDefectiveData, MapDefectiveSPCScheme, MapDefectiveChoiceKey, MapDefectiveTextKey, RecordChoiceDefective, RecordTextDefective, RecordCauseDefective,MapSPCScheme, MapDefectiveSPCScheme, MapDefectiveChoiceKey, MapDefectiveTextKey, MapSpecChoiceKey,MapSpecTextKey,RecordChoiceDefective, RecordTextDefective, RecordCauseDefective,RecordChoiceIndividual,RecordTextIndividual,RecordCauseIndividual
from app1.logics import i_mr
import pytz
import pandas as pd
from django.utils import timezone
from rest_framework import status
from django.contrib.postgres.aggregates import ArrayAgg
from collections import defaultdict
from django.urls import reverse
import numpy as np
import math

# Nelson spc rule violation logic
def check_spc_rule_1(data_array, upper_control_limit_array, lower_control_limit_array):
    points_above_k_control_line_sd = np.flatnonzero(data_array > upper_control_limit_array)
    points_below_k_control_line_sd = np.flatnonzero(data_array < lower_control_limit_array)
    last_index = len(data_array)-1
    last_index_alert = False
    if last_index in points_above_k_control_line_sd or last_index in points_below_k_control_line_sd:
        last_index_alert = True
    return{
        'points_above_k_control_line_sd': points_above_k_control_line_sd.tolist(),
        'points_below_k_control_line_sd': points_below_k_control_line_sd.tolist(),
        'last_index_alert1' : last_index_alert,
    }

def check_spc_rule_2(data_array, center_line_array, k_r2 = 9):
    k_points_above_cl = set()
    k_points_below_cl = set()
    k_r2 = int(k_r2)
    points_above_cl = np.flatnonzero(data_array > center_line_array)
    points_below_cl = np.flatnonzero(data_array < center_line_array)

    len_points_above_cl = int(len(points_above_cl))
    len_points_below_cl = int(len(points_below_cl))

    if len_points_above_cl >= k_r2:
        i = 0
        while i <= len_points_above_cl - k_r2:
            if points_above_cl[i+k_r2-1] == points_above_cl[i] + k_r2 -1:
                for j in range(i,i+k_r2):
                    k_points_above_cl.add(points_above_cl[j])
            i+=1

    if len_points_below_cl >= k_r2:
        i = 0
        while i <= len_points_below_cl - k_r2:
            if points_below_cl[i+k_r2-1] == points_below_cl[i] + k_r2 -1:
                for j in range(i,i+k_r2):
                    k_points_below_cl.add(points_below_cl[j])
            i+=1
    k_points_above_cl = list(k_points_above_cl)
    k_points_below_cl = list(k_points_below_cl)
    k_points_above_cl.sort()
    k_points_below_cl.sort()
    k_points_above_cl = np.array(k_points_above_cl)
    k_points_below_cl = np.array(k_points_below_cl)
    last_index = len(data_array)-1
    last_index_alert = False
    if last_index in k_points_above_cl or last_index in k_points_below_cl:
        last_index_alert = True
    return {
        'k_points_above_cl': k_points_above_cl.tolist(),
        'k_points_below_cl': k_points_below_cl.tolist(),
        'last_index_alert2': last_index_alert,
    }

def check_spc_rule_3_4(data_array, k_r3=6, k_r4=14, check_rule_3 =True, check_rule_4 = True):
    k_r3 = int(k_r3)
    k_r4 = int(k_r4)
    data_array_len = len(data_array)
    last_index = len(data_array)-1
    last_index_alert3 = False
    last_index_alert4 = False
    return_dict = {
        'k_points_increasing': [],
        'k_points_decreasing': [],
        'k_points_alternating': []
    }

    r_3_4_slope = np.zeros(data_array_len-1, dtype=int)
    for i in range(data_array_len -1):
        if data_array[i] < data_array[i+1]:
            r_3_4_slope[i] = 1
        elif  data_array[i] > data_array[i+1]:
            r_3_4_slope[i] = -1

    if check_rule_3 ==True:
        k_points_increasing = set()
        k_points_deceasing = set()
        for i in range(data_array_len - k_r3+1):
            streak = 0
            for j in range(i,i+k_r3-1):
                streak= streak + r_3_4_slope[j]
            if streak == k_r3-1:
                for j in range(i,i+k_r3):
                    k_points_increasing.add(j)
            elif streak == -k_r3+1:
                for j in range (i, i+k_r3):
                        k_points_deceasing.add(j)
        k_points_increasing = list(k_points_increasing)
        k_points_deceasing = list(k_points_deceasing)
        k_points_increasing.sort()
        k_points_deceasing.sort()
        k_points_increasing = np.array(k_points_increasing)
        k_points_deceasing = np.array(k_points_deceasing)
        if last_index in k_points_increasing or last_index in k_points_deceasing:
            last_index_alert3 = True
        return_dict['k_points_increasing'] = k_points_increasing.tolist()
        return_dict['k_points_decreasing'] = k_points_deceasing.tolist()
        return_dict['last_index_alert3'] = last_index_alert3
    
    if check_rule_4 == True:
        k_points_alternating = set()
        for i in range(data_array_len - k_r4+1):
            streak = 0
            for j in range(i,i+k_r4-1):
                streak= -1*(streak + r_3_4_slope[j])
            abs_streak = abs(streak)
            if abs_streak == k_r4-1:
                for j in range(i,i+k_r4):
                    k_points_alternating.add(j)

        k_points_alternating = list(k_points_alternating)
        k_points_alternating.sort()
        k_points_alternating = np.array(k_points_alternating)
        if last_index in k_points_alternating:
            last_index_alert4 = True
        return_dict['k_points_alternating'] = k_points_alternating.tolist()
        return_dict['last_index_alert4'] = last_index_alert4
    
    return return_dict

def check_spc_rule_5_6(data_array, center_line_array, control_line_sd_array, k_r5=2, k_r6=4, check_rule_5=True, check_rule_6=True):
    data_np_array = np.array(data_array)
    center_line_np_array = np.array(center_line_array)
    control_line_sd_np_array = np.array(control_line_sd_array)
    data_array_len = len(data_np_array)
    last_index = len(data_array)-1
    last_index_alert5 = False
    last_index_alert6 = False    
    return_dict = {
        'k_of_k_plus_1_points_above_2sd': [],
        'k_of_k_plus_1_points_below_2sd': [],
        'k_of_k_plus_1_points_above_1sd': [],
        'k_of_k_plus_1_points_below_1sd': []
    }

    if check_rule_5 == True:
        k_r5 = int(k_r5)
        r5_u_threshold_array = center_line_np_array + 2*control_line_sd_np_array
        r5_l_threshold_array = center_line_np_array - 2*control_line_sd_np_array
        k_of_k_plus_1_points_above_2sd = set()
        k_of_k_plus_1_points_below_2sd = set()

        for i in range(data_array_len-k_r5):
            upper_streak_count = 0
            upper_streak_index = set()
            lower_streak_count = 0
            lower_streak_index = set()
            for j in range(i,i+k_r5+1):
                if data_np_array[j] > r5_u_threshold_array[j]:
                    upper_streak_count+=1
                    upper_streak_index.add(j)
                elif data_np_array[j] <r5_l_threshold_array[j]:
                    lower_streak_count+=1
                    lower_streak_index.add(j)
            if upper_streak_count >= k_r5:
                k_of_k_plus_1_points_above_2sd = k_of_k_plus_1_points_above_2sd.union(upper_streak_index)
            if lower_streak_count >= k_r5:
                k_of_k_plus_1_points_below_2sd = k_of_k_plus_1_points_below_2sd.union(lower_streak_index)
        
        k_of_k_plus_1_points_above_2sd = list(k_of_k_plus_1_points_above_2sd)
        k_of_k_plus_1_points_above_2sd.sort()
        k_of_k_plus_1_points_above_2sd = np.array(k_of_k_plus_1_points_above_2sd)
        return_dict['k_of_k_plus_1_points_above_2sd'] = k_of_k_plus_1_points_above_2sd.tolist()
        k_of_k_plus_1_points_below_2sd = list(k_of_k_plus_1_points_below_2sd)
        k_of_k_plus_1_points_below_2sd.sort()
        k_of_k_plus_1_points_below_2sd = np.array(k_of_k_plus_1_points_below_2sd)
        return_dict['k_of_k_plus_1_points_below_2sd'] = k_of_k_plus_1_points_below_2sd.tolist()
        if last_index in k_of_k_plus_1_points_above_2sd or last_index in k_of_k_plus_1_points_below_2sd:
            last_index_alert5 = True
        return_dict['last_index_alert5'] = last_index_alert5

    if check_rule_6 == True:
        k_r6= int(k_r6)
        r6_u_threshold_array = center_line_np_array + 1*control_line_sd_np_array
        r6_l_threshold_array = center_line_np_array - 1*control_line_sd_np_array
        k_of_k_plus_1_points_above_1sd = set()
        k_of_k_plus_1_points_below_1sd = set()
        for i in range(data_array_len-k_r6):
            upper_streak_count = 0
            upper_streak_index = set()
            lower_streak_count = 0
            lower_streak_index = set()
            for j in range(i,i+k_r6+1):
                if data_array[j] > r6_u_threshold_array[j]:
                    upper_streak_count+=1
                    upper_streak_index.add(j)
                elif data_array[j] < r6_l_threshold_array[j]:
                    lower_streak_count+=1
                    lower_streak_index.add(j)
            if upper_streak_count >= k_r6:
                k_of_k_plus_1_points_above_1sd = k_of_k_plus_1_points_above_1sd.union(upper_streak_index)
            if lower_streak_count >= k_r6:
                k_of_k_plus_1_points_below_1sd = k_of_k_plus_1_points_below_1sd.union(lower_streak_index)
        k_of_k_plus_1_points_above_1sd = list(k_of_k_plus_1_points_above_1sd)
        k_of_k_plus_1_points_below_1sd = list(k_of_k_plus_1_points_below_1sd)
        k_of_k_plus_1_points_above_1sd.sort()
        k_of_k_plus_1_points_below_1sd.sort()
        k_of_k_plus_1_points_below_1sd = np.array(k_of_k_plus_1_points_below_1sd)
        k_of_k_plus_1_points_above_1sd = np.array(k_of_k_plus_1_points_above_1sd)
        if last_index in k_of_k_plus_1_points_below_1sd or last_index in k_of_k_plus_1_points_above_1sd:
            last_index_alert6 = True
        return_dict['k_of_k_plus_1_points_above_1sd'] = k_of_k_plus_1_points_above_1sd.tolist()
        return_dict['k_of_k_plus_1_points_below_1sd'] = k_of_k_plus_1_points_below_1sd.tolist()
        return_dict['last_index_alert6'] = last_index_alert6
    return return_dict

def check_spc_rule_7_8(data_array, center_line_array, control_line_sd_array, k_r7=15, k_r8=8, check_rule_7 = True, check_rule_8 = True):
    data_np_array = np.array(data_array)
    center_line_np_array = np.array(center_line_array)
    control_line_sd_np_array = np.array(control_line_sd_array)
    upper_1sd_array = center_line_np_array + 1*control_line_sd_np_array
    lower_1sd_array = center_line_np_array - 1*control_line_sd_np_array
    last_index = len(data_array)-1
    last_index_alert7 = False
    last_index_alert8 = False   

    return_dict = {
        'k_points_between_1sds': [],
        'k_points_outside_1sds': []
    }

    if check_rule_7 ==True:
        points_between_1sds = np.flatnonzero(np.logical_and(data_np_array>lower_1sd_array, data_np_array < upper_1sd_array))
        len_points_between_1sd = int(len(points_between_1sds))
        k_points_between_1sds = set()
        if len_points_between_1sd >= k_r7:
            for i in range(len_points_between_1sd - k_r7+1):
                if points_between_1sds[i+k_r7-1] == points_between_1sds[i] + k_r7 -1:
                    for j in range(i,i+k_r7):
                        k_points_between_1sds.add(points_between_1sds[j])
        k_points_between_1sds = list(k_points_between_1sds)
        k_points_between_1sds.sort()
        k_points_between_1sds = np.array(k_points_between_1sds)
        if last_index in k_points_between_1sds:
            last_index_alert7 = True
        return_dict['k_points_between_1sds'] = k_points_between_1sds.tolist()
        return_dict['last_index_alert7'] = last_index_alert7
    
    if check_rule_8 == True:
        points_outside_1sds = np.flatnonzero(np.logical_or(data_np_array<lower_1sd_array, data_np_array > upper_1sd_array))
        len_points_outside_1sd = int(len(points_outside_1sds))
        k_points_outside_1sds = set()
        if len_points_outside_1sd >= k_r8:
            for i in range(len_points_outside_1sd - k_r8+1):
                if points_outside_1sds[i+k_r8-1] == points_outside_1sds[i] + k_r8 -1:
                    for j in range(i,i+k_r8):
                        k_points_outside_1sds.add(points_outside_1sds[j])
        k_points_outside_1sds = list(k_points_outside_1sds)
        k_points_outside_1sds.sort()
        k_points_outside_1sds = np.array(k_points_outside_1sds)
        if last_index in k_points_outside_1sds:
            last_index_alert8 = True
        return_dict['k_points_outside_1sds'] = k_points_outside_1sds.tolist()
        return_dict['last_index_alert8'] = last_index_alert8

    return return_dict

# Updated plot_spc_rules to return violations dict with lists
def plot_spc_rules(data_array, center_line_array, control_line_sd_array, upper_control_limit_array, lower_control_limit_array, k_r1=3, k_r2=9, k_r3=6, k_r4=14, k_r5=2, k_r6=4, k_r7=15, k_r8=8):
    spc_rule1 = check_spc_rule_1(data_array, upper_control_limit_array, lower_control_limit_array)
    spc_rule2 = check_spc_rule_2(data_array, center_line_array, k_r2)
    spc_rule3_4 = check_spc_rule_3_4(data_array, k_r3, k_r4)
    spc_rule5_6 = check_spc_rule_5_6(data_array, center_line_array, control_line_sd_array, k_r5, k_r6)
    spc_rule7_8 = check_spc_rule_7_8(data_array, center_line_array, control_line_sd_array, k_r7, k_r8)
    
    violations = {
        'rule1_above': spc_rule1['points_above_k_control_line_sd'],
        'rule1_below': spc_rule1['points_below_k_control_line_sd'],
        'rule2_above': spc_rule2['k_points_above_cl'],
        'rule2_below': spc_rule2['k_points_below_cl'],
        'rule3_increasing': spc_rule3_4['k_points_increasing'],
        'rule3_decreasing': spc_rule3_4['k_points_decreasing'],
        'rule4_alternating': spc_rule3_4['k_points_alternating'],
        'rule5_above': spc_rule5_6['k_of_k_plus_1_points_above_2sd'],
        'rule5_below': spc_rule5_6['k_of_k_plus_1_points_below_2sd'],
        'rule6_above': spc_rule5_6['k_of_k_plus_1_points_above_1sd'],
        'rule6_below': spc_rule5_6['k_of_k_plus_1_points_below_1sd'],
        'rule7_within': spc_rule7_8['k_points_between_1sds'],
        'rule8_outside': spc_rule7_8['k_points_outside_1sds'],
    }
    return violations

class RealtimePChartDataView(APIView):
    permission_classes = [IsAuthenticated]  # Require authentication
    def get(self, request, scheme_id):
        response_data = {}
        scheme = MapDefectiveSPCScheme.objects.get(pk=scheme_id)
        workunit_timezone = scheme.equipment.work_unit_id.timezone
        tz = pytz.timezone(workunit_timezone)
        last_n_records = 31
        # Get last_n_records from query params (default 31)
        try:
            last_n_records = int(request.query_params.get('last_n_records', 31))
        except Exception as e:
            pass
        if last_n_records > 3000:
            last_n_records = 3000
        records = RecordDefectiveData.objects.filter(
            part=scheme.part, equipment=scheme.equipment, defective_test=scheme.defective_test
        ).order_by('-sample_date_time')[:last_n_records]
       
        if len(records)<=4:
            response_data['error_msg'] = "Not Enough Data to show the realtime P chart."
            return Response(response_data)
        
        # Convert records to DataFrame
        records_df = pd.DataFrame(records.values('id', 'sample_size', 'defective_count', 'sample_date_time'))
        records_df['sample_date_time'] = records_df['sample_date_time'].dt.tz_convert(workunit_timezone).dt.tz_localize(None)
        records_df = records_df.sort_values(by='sample_date_time').reset_index(drop=True)
        records_df['sample_date'] = records_df['sample_date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Prepare data for frontend
        data = []
        for _, row in records_df.iterrows():
            data.append({
                'defective_count': row['defective_count'],
                'sample_size': row['sample_size'],
                'sample_date_time': row['sample_date'],
            })

        use_historical_p = request.query_params.get('use_historical_p', 'false').lower() == 'true'
        include_raw_data = request.query_params.get('include_raw_data', 'false').lower() == 'true'
        historical_p = scheme.historical_defective_rate if use_historical_p and scheme.historical_defective_rate is not None else None
        response_data = {'data': data}
        if historical_p is not None:
            response_data['historical_p'] = historical_p

        if include_raw_data:
            record_ids = [rec.id for rec in records]
            scheme_records_df = pd.DataFrame([{
                'id': rec.id,
                'sample_size': rec.sample_size,
                'defective_count': rec.defective_count,
                'sample_date_time': rec.sample_date_time.astimezone(tz).strftime('%Y-%m-%d %H:%M:%S'),
                'updation_time': rec.updation_time.astimezone(tz).strftime('%Y-%m-%d %H:%M:%S'),
                'sample_id': rec.sample_id,
                'lot_no': rec.lot_no,
                'remarks': rec.remarks,
            } for rec in records])


            for i in scheme_records_df.index:
                hl = reverse('admin:app1_recorddefectivedata_change', args=[scheme_records_df.loc[i, 'id']])
                scheme_records_df.loc[i, 'Edit'] = f'<a href="{hl}" target="_blank">Edit</a>'

            defective_test = scheme.defective_test
            relevant_choice_keys = MapDefectiveChoiceKey.objects.filter(spec=defective_test).values_list('choice_key__name', flat=True).distinct()
            relevant_text_keys = MapDefectiveTextKey.objects.filter(spec=defective_test).values_list('text_key__name', flat=True).distinct()

            for choice_key_name in relevant_choice_keys:
                scheme_records_df[choice_key_name] = None
            for text_key_name in relevant_text_keys:
                scheme_records_df[text_key_name] = None

            choice_data = RecordChoiceDefective.objects.filter(data_id__in=record_ids).select_related('choice')
            text_data = RecordTextDefective.objects.filter(data_id__in=record_ids).select_related('key')

            for choice in choice_data:
                choice_key_name = choice.choice.key.name
                if choice_key_name in relevant_choice_keys:
                    scheme_records_df.loc[scheme_records_df['id'] == choice.data_id, choice_key_name] = choice.choice.name
            for text in text_data:
                text_key_name = text.key.name
                if text_key_name in relevant_text_keys:
                    scheme_records_df.loc[scheme_records_df['id'] == text.data_id, text_key_name] = text.value

            cause_data = RecordCauseDefective.objects.filter(data_id__in=record_ids).values('data_id', 'cause__name')
            causes_dict = defaultdict(list)
            for cd in cause_data:
                causes_dict[cd['data_id']].append(cd['cause__name'])
            scheme_records_df['causes'] = scheme_records_df['id'].apply(lambda x: causes_dict.get(x, []))

            raw_data = scheme_records_df[::-1].to_dict(orient='records')
            response_data['raw_data'] = raw_data
            response_data['relevant_choice_keys'] = [str(key) for key in relevant_choice_keys]
            response_data['relevant_text_keys'] = [str(key) for key in relevant_text_keys]

        return Response(response_data)

class PChartDataByPeriodView(APIView):
    def post(self, request, scheme_id):
        scheme = MapDefectiveSPCScheme.objects.get(pk=scheme_id)
        defective_test = scheme.defective_test
        workunit_timezone = scheme.equipment.work_unit_id.timezone
        tz = pytz.timezone(workunit_timezone)
        start_date_str = request.data.get('start_date_time')
        end_date_str = request.data.get('end_date_time')
        use_historical_p = request.data.get('use_historical_p', 'false').lower() == 'on'
        display_raw_data = request.data.get('display_raw_data', 'false').lower() == 'on'
        
        if not start_date_str or not end_date_str:
            return Response({'error': 'Start date and end date are required.'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            start_date_naive = timezone.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M')
            start_date = tz.localize(start_date_naive)
            end_date_naive = timezone.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M')
            end_date = tz.localize(end_date_naive)
        except ValueError:
            return Response({'error': 'Invalid date format.'}, status=status.HTTP_400_BAD_REQUEST)
        
        records = RecordDefectiveData.objects.filter(
            part=scheme.part,
            equipment=scheme.equipment,
            defective_test=scheme.defective_test,
            sample_date_time__range=(start_date, end_date)
        ).order_by('-sample_date_time')[:3000]
        
        if records.count() > 3000:
            return Response({'error': 'Too many data points. Please select a smaller time period.'}, status=status.HTTP_400_BAD_REQUEST)
        
        data = []
        for rec in records:
            local_time = rec.sample_date_time.astimezone(tz)
            formatted_time = local_time.strftime('%Y-%m-%d %H:%M:%S')
            data.append({
                'defective_count': rec.defective_count,
                'sample_size': rec.sample_size,
                'sample_date_time': formatted_time
            })
        historical_p = scheme.historical_defective_rate if use_historical_p and scheme.historical_defective_rate is not None else None
        response_data = {'data': data}
        if historical_p is not None:
            response_data['historical_p'] = historical_p

        if use_historical_p:
            process_proportion = scheme.historical_defective_rate
        else:
            process_proportion = None
        
        records = records.annotate(
            causes=ArrayAgg('recordcausedefective__cause__name', distinct=True)
        )
        scheme_records_df = pd.DataFrame(records.values('id','sample_size', 'defective_count', 'sample_date_time','updation_time', 'sample_id', 'lot_no', 'remarks'))
        if len(scheme_records_df.index)>0:
            scheme_records_df['sample_date_time'] = scheme_records_df['sample_date_time'].dt.tz_convert(workunit_timezone).dt.tz_localize(None)
            scheme_records_df['updation_time'] = scheme_records_df['updation_time'].dt.tz_convert(workunit_timezone).dt.tz_localize(None)

            if display_raw_data:
                for i in scheme_records_df.index:
                    hl = reverse('admin:app1_recorddefectivedata_change', args=[scheme_records_df.loc[i,'id'],])
                    scheme_records_df.loc[i,'Edit'] = f'<a href="{hl}">Edit</a>'
                # context['scheme_records_df_html'] = scheme_records_df.to_html(index=False, render_links=True, escape=False, classes = "table table-bordered")
                scheme_records_df['sample_date']=scheme_records_df['sample_date_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                scheme_records_df['updation_date']=scheme_records_df['updation_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                # Get the relevant choice keys and text keys from MapDefectiveChoiceKey and MapDefectiveTextKey
                relevant_choice_keys = MapDefectiveChoiceKey.objects.filter(
                    spec=defective_test
                ).values_list('choice_key__name', flat=True).distinct()

                relevant_text_keys = MapDefectiveTextKey.objects.filter(
                    spec=defective_test
                ).values_list('text_key__name', flat=True).distinct()

                # Initialize columns for the choice keys and text keys in the DataFrame
                for choice_key_name in relevant_choice_keys:
                    scheme_records_df[choice_key_name] = None  # Add a column for each choice key
                
                for text_key_name in relevant_text_keys:
                    scheme_records_df[text_key_name] = None  # Add a column for each text key

                # Get all RecordChoiceDefective and RecordTextDefective entries related to RecordDefectiveData
                record_ids = records.values_list('id', flat=True)
                choice_data = RecordChoiceDefective.objects.filter(data_id__in=record_ids).select_related('choice')
                text_data = RecordTextDefective.objects.filter(data_id__in=record_ids).select_related('key')

                # Populate the DataFrame with the corresponding choice values
                for choice in choice_data:
                    choice_key_name = choice.choice.key.name
                    if choice_key_name in relevant_choice_keys:
                        scheme_records_df.loc[scheme_records_df['id'] == choice.data_id, choice_key_name] = choice.choice.name

                # Populate the DataFrame with the corresponding text values
                for text in text_data:
                    text_key_name = text.key.name
                    if text_key_name in relevant_text_keys:
                        scheme_records_df.loc[scheme_records_df['id'] == text.data_id, text_key_name] = text.value
                # context['relevant_choice_keys'] = list(relevant_choice_keys)
                # context['relevant_text_keys'] = list(relevant_text_keys)
                # context['scheme_records_df'] = scheme_records_df.to_json(orient='records')
            

        return Response(response_data)
    
# Full updated RefreshIMRChartDataView
class RefreshIMRChartDataView(APIView):
    permission_classes = [IsAuthenticated]

    def clean_floats(self, data):
        if isinstance(data, dict):
            return {k: self.clean_floats(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.clean_floats(item) for item in data]
        elif isinstance(data, float):
            if math.isnan(data) or math.isinf(data):
                return None
            return data
        else:
            return data

    def get(self, request, scheme_id):
        response_data = {}
        scheme = MapSPCScheme.objects.get(pk=scheme_id)
        workunit_timezone = scheme.equipment_id.work_unit_id.timezone
        tz = pytz.timezone(workunit_timezone)
        last_n_records = 31
        # Get last_n_records from query params (default 31)
        try:
            last_n_records = int(request.query_params.get('last_n_records', 31))
        except Exception as e:
            pass
        if last_n_records > 3000:
            last_n_records = 3000
        records = RecordImrIndividualData.objects.select_related(
            'operator', 'inspector', 'gauge'
        ).filter(
            map_spc_scheme_id=scheme_id
        ).order_by('-time_stamp')[:last_n_records]

        data = []
        data_points = []
        # for rec in records:
        #     local_time = rec.time_stamp.astimezone(tz)
        #     formatted_time = local_time.strftime('%Y-%m-%d %H:%M:%S')
        #     data.append({
        #         'data_point': rec.data_point,
        #         'time_stamp': formatted_time
        #     })
        #     data_points.append(rec.data_point)

        if len(records) <= 4:
            response_data['error_msg'] = "Not Enough Data to show the realtime I chart."
            return Response(response_data)

        # Convert queryset into dataframe
        records_df = pd.DataFrame(records.values(
            'id', 'data_point', 'time_stamp'
        ))

        # Convert timezone and format
        records_df['time_stamp'] = records_df['time_stamp'].dt.tz_convert(workunit_timezone).dt.tz_localize(None)
        # records_df = records_df.sort_values(by='time_stamp').reset_index(drop=True)
        records_df['time_stamp_str'] = records_df['time_stamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data_points = records_df['data_point'].values.tolist()

        # Prepare data for frontend
        data = []
        # data_points = []
        for _, row in records_df.iterrows():
            data.append({
                'data_point': row['data_point'],
                'time_stamp': row['time_stamp_str']
            })
        
        if len(records_df)>=4:
            id_links = records_df['id'].values.tolist()[::-1]
        else:
            id_links = []

        use_historical_mu = request.query_params.get('use_historical_mu', 'false').lower() == 'true'
        use_historical_sigma = request.query_params.get('use_historical_sigma', 'false').lower() == 'true'
        include_raw_data = request.query_params.get('include_raw_data', 'false').lower() == 'true'
        historical_mu = scheme.historical_mu if use_historical_mu and scheme.historical_mu is not None else None
        historical_sigma = scheme.historical_sigma if use_historical_sigma and scheme.historical_sigma is not None else None
        usl = scheme.part_vc_id.upper_spec_limit
        lsl = scheme.part_vc_id.lower_spec_limit

        if not isinstance(usl, (int, float)) or np.isnan(usl) or np.isinf(usl):
            usl = None
        if not isinstance(lsl, (int, float)) or np.isnan(lsl) or np.isinf(lsl):
            lsl = None

        imr_data = i_mr(
            data_points[::-1],
            historical_mean=historical_mu,
            historical_s=historical_sigma,
            usl=usl,
            lsl=lsl
        )

        controllines = {
            'i_cl': imr_data['i_cl'][0],
            'i_ucl': imr_data['i_ucl'][0],
            'i_lcl': imr_data['i_lcl'][0],
            'i_2su': imr_data['i_2su'][0],
            'i_2sl': imr_data['i_2sl'][0],
            'usl': imr_data['usl'][0] if 'usl' in imr_data and imr_data['usl'][0] is not None else None,
            'lsl': imr_data['lsl'][0] if 'lsl' in imr_data and imr_data['lsl'][0] is not None else None,
            'mr_cl': imr_data['mr_cl'][0],
            'mr_lcl': imr_data['mr_lcl'][0],
            'mr_ucl': imr_data['mr_ucl'][0],
        }

        template_filter = scheme.part_vc_id
        settings = {
            'show_usl': template_filter.alert_template.show_usl,
            'show_lsl': template_filter.alert_template.show_lsl,
            'show_3sl_upper': template_filter.alert_template.show_3sl_upper,
            'show_3sl_lower': template_filter.alert_template.show_3sl_lower,
            'show_2sl_upper': template_filter.alert_template.show_2sl_upper,
            'show_2sl_lower': template_filter.alert_template.show_2sl_lower,
            'rule_1_b': template_filter.alert_template.x_rule_1_b,
            'show_3sl_upper_alerts': template_filter.alert_template.show_3sl_upper_alerts,
            'show_3sl_lower_alerts': template_filter.alert_template.show_3sl_lower_alerts,
            'legend_decimals':template_filter.alert_template.legend_decimals,
            'rule_2_b':template_filter.alert_template.x_rule_2_b, 
            'rule_3_b':template_filter.alert_template.x_rule_3_b, 
            'rule_4_b':template_filter.alert_template.x_rule_4_b, 
            'rule_5_b':template_filter.alert_template.x_rule_5_b, 
            'rule_6_b':template_filter.alert_template.x_rule_6_b,
            'rule_7_b':template_filter.alert_template.x_rule_7_b, 
            'rule_8_b':template_filter.alert_template.x_rule_8_b,
            'r_s_rule_1_b': template_filter.alert_template.r_s_rule_1_b,
            'r_s_rule_2_b': template_filter.alert_template.r_s_rule_2_b,
            'r_s_rule_3_b': template_filter.alert_template.r_s_rule_3_b,
            'r_s_rule_4_b': template_filter.alert_template.r_s_rule_4_b,
        }

        timestamps = [d['time_stamp'] for d in data]
        chronological_timestamps = timestamps[::-1]
        mr_timestamps = chronological_timestamps
        mr_data_with_time = []
        for ts, mr_value in zip(mr_timestamps, imr_data['mr']):
            mr_data_with_time.append({
                'time_stamp': ts,
                'mr': mr_value
            })
        mr_data_with_time = mr_data_with_time[::-1]

        response_data = {'i_data': data, 'mr_data': mr_data_with_time, 'controllines': controllines, 'settings': settings, 'id_links':id_links}
        if historical_mu is not None:
            response_data['historical_mu'] = historical_mu
        if historical_sigma is not None:
            response_data['historical_sigma'] = historical_sigma

        # Compute violations (ascending)
        i_data_array = np.array(data_points[::-1])
        i_cl_array = np.full(len(i_data_array), controllines['i_cl'])
        i_sd_array = np.full(len(i_data_array), historical_sigma if historical_sigma else (controllines['i_ucl'] - controllines['i_cl']) / 3)
        i_ucl_array = np.full(len(i_data_array), controllines['i_ucl'])
        i_lcl_array = np.full(len(i_data_array), controllines['i_lcl'])
        response_data['i_violations'] = plot_spc_rules(i_data_array, i_cl_array, i_sd_array, i_ucl_array, i_lcl_array)

        mr_data_array = imr_data['mr']
        mr_cl_array = np.full(len(mr_data_array), controllines['mr_cl'])
        mr_sd_array = np.full(len(mr_data_array), 0)  # unused for MR
        mr_ucl_array = np.full(len(mr_data_array), controllines['mr_ucl'])
        mr_lcl_array = np.full(len(mr_data_array), controllines['mr_lcl'])
        response_data['mr_violations'] = plot_spc_rules(mr_data_array, mr_cl_array, mr_sd_array, mr_ucl_array, mr_lcl_array)

        if include_raw_data:
            raw_data_list = []
            for rec in records:
                local_time = rec.time_stamp.astimezone(tz)
                upd_local = rec.updation_time.astimezone(tz)
                raw_data_list.append({
                    'id': rec.id,
                    'data_point': rec.data_point,
                    'time_stamp': local_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'updation_time': upd_local.strftime('%Y-%m-%d %H:%M:%S'),
                    'sample_id': rec.sample_id,
                    'lot_no': rec.lot_no,
                    'remarks': rec.remarks,
                    'operator': rec.operator.name if rec.operator else None,
                    'inspector': rec.inspector.name if rec.inspector else None,
                    'gauge': rec.gauge.name if rec.gauge else None,
                })
            scheme_records_df = pd.DataFrame(raw_data_list)

            scheme_records_df['Edit'] = scheme_records_df['id'].apply(
                lambda x: f'<a href="{reverse("admin:app1_recordimrindividualdata_change", args=[x])}" target="_blank">Edit</a>'
            )

            part_var_spec = scheme.part_vc_id
            relevant_choice_keys = list(MapSpecChoiceKey.objects.filter(spec=part_var_spec).values_list('choice_key__name', flat=True).distinct())
            relevant_text_keys = list(MapSpecTextKey.objects.filter(spec=part_var_spec).values_list('text_key__name', flat=True).distinct())

            for choice_key_name in relevant_choice_keys:
                scheme_records_df[choice_key_name] = None
            for text_key_name in relevant_text_keys:
                scheme_records_df[text_key_name] = None

            record_ids = scheme_records_df['id'].tolist()
            choice_data = RecordChoiceIndividual.objects.filter(data_id__in=record_ids).select_related('choice__key')
            text_data = RecordTextIndividual.objects.filter(data_id__in=record_ids).select_related('key')
            cause_data = RecordCauseIndividual.objects.filter(data_id__in=record_ids).values('data_id', 'cause__name')

            for choice in choice_data:
                choice_key_name = choice.choice.key.name
                if choice_key_name in relevant_choice_keys:
                    scheme_records_df.loc[scheme_records_df['id'] == choice.data_id, choice_key_name] = choice.choice.name
            for text in text_data:
                text_key_name = text.key.name
                if text_key_name in relevant_text_keys:
                    scheme_records_df.loc[scheme_records_df['id'] == text.data_id, text_key_name] = text.value

            causes_dict = defaultdict(list)
            for cd in cause_data:
                causes_dict[cd['data_id']].append(cd['cause__name'])
            scheme_records_df['causes'] = scheme_records_df['id'].apply(lambda x: causes_dict.get(x, []))

            scheme_records_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            scheme_records_df.replace(np.nan, None, inplace=True)

            raw_data = scheme_records_df.to_dict(orient='records')
            response_data['raw_data'] = raw_data
            response_data['relevant_choice_keys'] = relevant_choice_keys
            response_data['relevant_text_keys'] = relevant_text_keys

        response_data = self.clean_floats(response_data)

        return Response(response_data)