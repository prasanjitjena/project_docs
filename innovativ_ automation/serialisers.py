
from rest_framework import serializers
from .models import RecordImrIndividualData, MapSPCScheme,RecordSPCSubgroupData,RecordChoiceSubgroup,MapSPCScheme,MapPartVariableCharacteristics,ConfChoiceKey,ConfChoiceValue
import traceback
import pandas as pd

class RecordImrIndividualDataSerializer(serializers.ModelSerializer):
    tag_id_default = serializers.CharField(write_only=True)
    tag_id_backup = serializers.CharField(write_only=True)

    class Meta:
        model = RecordImrIndividualData
        exclude = ['map_spc_scheme_id']
        extra_kwargs = {
            'data_point': {'required': False},  # Marking data_point as not required
            "tag_id_backup": {'required': False},
            "data_point_backup": {'required': False},
            "condition_1":{'required': False},
            "condition_2":{'required': False},
            "condition_3":{'required': False},
            "condition_4":{'required': False},
        }

    def create(self, validated_data_list):
        if not isinstance(validated_data_list, list):
            validated_data_list = [validated_data_list]  # Convert to list if single object

        instances = []  # List to collect objects for bulk insert
        for validated_data in validated_data_list:
            tag_id_default = validated_data.pop('tag_id_default', None)
            data_point_default = validated_data.pop('data_point_default', None)
            tag_id_backup = None
            data_point_backup = None
            map_spc_scheme = None
            save_data = False
            condition = 0
            condition_1 = 0
            condition_2 = 0
            condition_3 = 0
            condition_4 = 0

            if 'condition_1' in validated_data:
                condition_1 = validated_data['condition_1']

            if 'condition_2' in validated_data:
                condition_2 = validated_data['condition_2']

            if 'condition_3' in validated_data:
                condition_3 = validated_data['condition_3']
            
            if 'condition_4' in validated_data:
                condition_4 = validated_data['condition_4']

            condition = condition_1 + condition_2 + condition_3 + condition_3 + condition_4
            condition = 1
            if condition > 0:
                save_data = True

            if save_data:
                if 'tag_id_backup' in validated_data:
                    tag_id_backup = validated_data.pop('tag_id_backup', None)
                    data_point_backup = validated_data.pop('data_point_backup', None)
                
                if tag_id_default:
                    try:
                        map_spc_scheme = MapSPCScheme.objects.get(tag_id_default=tag_id_default)
                        validated_data['map_spc_scheme_id'] = map_spc_scheme
                        
                    except MapSPCScheme.DoesNotExist:
                        pass  # Ignore cases where the provided scheme_id_str doesn't match any existing MapSPCScheme
                elif tag_id_backup:
                    try:
                        map_spc_scheme = MapSPCScheme.objects.get(tag_id_backup=tag_id_backup)
                        validated_data['map_spc_scheme_id'] = map_spc_scheme
                    except MapSPCScheme.DoesNotExist:
                        pass  # Ignore cases where the provided scheme_id_str doesn't match any existing MapSPCScheme
                
                # Compare data_point_default and data_point_backup and set the highest value to data_point
                if map_spc_scheme:
                    if data_point_default is not None and data_point_backup is not None:
                        validated_data['data_point'] = max(data_point_default, data_point_backup)
                        validated_data['data_point_default'] = data_point_default
                        validated_data['data_point_backup'] = data_point_backup
                    elif data_point_default is not None:
                        validated_data['data_point'] = data_point_default
                        validated_data['data_point_default'] = data_point_default
                    elif data_point_backup is not None:
                        validated_data['data_point'] = data_point_backup
                        validated_data['data_point_backup'] = data_point_backup
                    
                    # return super().create(validated_data)
                    instances.append(RecordImrIndividualData(**validated_data))
                else:
                    return None
            else:
                return None
        if instances:
            return RecordImrIndividualData.objects.bulk_create(instances)

