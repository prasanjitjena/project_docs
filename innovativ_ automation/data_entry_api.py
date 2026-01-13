from app1.models import RecordImrIndividualData, RecordSPCSubgroupData
from app1.serializers import RecordImrIndividualDataSerializer, RecordSPCSubgroupDataSerializer, RecordImrIndividualDataGetSerializer,RecordCmmImrSerializer,RecordCmmImrSerializer_1,RecordIMRSerializer,RecordSPCSubgroupDataSerializer_1
from rest_framework import status, generics, authentication, response
from rest_framework.permissions import IsAuthenticated
from app1.views1.data_entry_views import trigger_email_alert
from rest_framework import serializers

class ImrDataEntryAPI(generics.CreateAPIView):
    queryset = RecordImrIndividualData.objects.all()
    serializer_class = RecordImrIndividualDataSerializer
    authentication_classes = [authentication.TokenAuthentication]
    permission_classes =[IsAuthenticated]

    def create(self, request, *args, **kwargs):
        # Check if data is provided in the request
        if not isinstance(request.data, list):
            return response.Response({"detail": "Data must be provided as a list."}, status=status.HTTP_400_BAD_REQUEST)
        
        serializer = self.get_serializer(data=request.data, many=True)
        serializer.is_valid(raise_exception=True)
        # self.perform_create(serializer)
        # headers = self.get_success_headers(serializer.data)
        # return response.Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)
        created_instances = [instance for sublist in serializer.save() for instance in (sublist if isinstance(sublist, list) else [sublist])]
        unique_instances = {instance.map_spc_scheme_id: instance for instance in created_instances}.values()
        headers = {}
        response_data = RecordCmmImrSerializer(unique_instances, many=True).data
        for instance in unique_instances:
            # record_saved.send(sender=RecordImrIndividualData, instance=instance, created=True, request=request)
            trigger_email_alert(instance,request)
        return response.Response(response_data, status=status.HTTP_201_CREATED, headers=headers)

