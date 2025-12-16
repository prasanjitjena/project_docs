


############################# SERIALIZER #########################################################################

class ChoiceMappingSerializer(serializers.Serializer):
    spec = serializers.PrimaryKeyRelatedField(queryset=MapPartVariableCharacteristics.objects.all())
    choice_key = serializers.CharField()
    value = serializers.CharField()

    def validate(self, attrs):
        key_name = attrs['choice_key']
        value_name = attrs['value']

        try:
            choice_key_obj = ConfChoiceKey.objects.get(name=key_name)
        except ConfChoiceKey.DoesNotExist:
            raise serializers.ValidationError({'choice_key': f'Invalid choice key: {key_name}'})

        try:
            value_obj = ConfChoiceValue.objects.get(key=choice_key_obj, name=value_name)
        except ConfChoiceValue.DoesNotExist:
            raise serializers.ValidationError({'value': f'Invalid value "{value_name}" for key "{key_name}"'})
        attrs['choice_key'] = choice_key_obj
        attrs['value'] = value_obj
        return attrs

class RecordSPCSubgroupDataSerializer(serializers.ModelSerializer):
    tag_id_default = serializers.CharField(write_only=True)
    individual_data_array = serializers.ListField(child=serializers.FloatField())
    mapchoicekey = ChoiceMappingSerializer(many=True, write_only=True)

    class Meta:
        model = RecordSPCSubgroupData
        fields = ['tag_id_default', 'individual_data_array', 'mapchoicekey','time_stamp']
    
    def validate_tag_id_default(self, value):
        try:
            return MapSPCScheme.objects.get(tag_id_default=value, store_as_subgroup=True)
        except MapSPCScheme.DoesNotExist:
            raise serializers.ValidationError("Invalid or unmatched tag_id_default.")


    def create(self, validated_data):
        print('vv',validated_data)
        map_scheme = validated_data.pop('tag_id_default')
        mapchoicekey_data = validated_data.pop('mapchoicekey')
        subgroup = RecordSPCSubgroupData.objects.create(map_spc_scheme_id=map_scheme,**validated_data)

        for item in mapchoicekey_data:
            spec = item['spec']
            choice_key = item['choice_key']
            value = item['value']  
            choice_data= RecordChoiceSubgroup.objects.create(data = subgroup,choice=value)
            print(f"Spec: {spec}, Key: {choice_key}, Value: {value}")
        return subgroup




################################################## VIEWS ######################################################


class RecordSubGroupPost(generics.CreateAPIView):
    # queryset = RecordImrIndividualData.objects.all()
    serializer_class = RecordSPCSubgroupDataSerializer
    # authentication_classes = [TokenAuthentication]
    # permission_classes = [IsAuthenticated]
    def create(self, request, *args, **kwargs):
        error=None
        data=request.data
        print(data,'dd')
        try:
            serializer = self.get_serializer(data=data, many=True)  
            serializer.is_valid(raise_exception=True)  
            instances = serializer.create(serializer.validated_data)  
        except serializers.ValidationError as e:
            print("Validation errors:", e.detail)
            error=e.detail  
        return Response(status=status.HTTP_201_CREATED,data=error )
    