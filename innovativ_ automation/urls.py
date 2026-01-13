from django.urls import path

urlpatterns = [
path('api/imr/', data_entry_apis.ImrDataEntryAPI.as_view(), name='imr_dataentry_api'),
]