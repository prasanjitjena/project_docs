from django.urls import path,include
from . import views, report_views
from app1.views1 import data_entry_views, scheme_list_views, analysis_views,autoemail_views,data_entry_apis,scheme_entry_views,tenant_views,bulk_user_creation,delete_views,export_views,analysis_api,custom_login
# import data_entry_defective, data_entry_defect 
from django.contrib.auth import views as auth_views
from django.contrib import admin
from django.http import HttpResponse

urlpatterns = [
    # only graph refresh api
    path('analysis/imr_chart_refresh/<int:scheme_id>/', analysis_views.imr_chart_refresh, name='realtime_imr_chart_refresh'),  
    path('api/schemes/<int:scheme_id>/refresh_imr_chart_data/', analysis_api.RefreshIMRChartDataView.as_view(), name='refresh_imr_chart_data'),
    path('analysis/realtime_pchart/<int:scheme_id>/', analysis_views.realtime_pchart_refresh, name='realtime_pchart_refresh'),  
    path('api/schemes/<int:scheme_id>/realtime_pchart_data/', analysis_api.RealtimePChartDataView.as_view(), name='realtime_pchart_data'),
    path('api/schemes/<int:scheme_id>/pchart_data_by_period/', analysis_api.PChartDataByPeriodView.as_view(), name='pchart_data_by_period'),

]