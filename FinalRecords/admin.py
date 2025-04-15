from django.contrib import admin
from django.http import HttpResponseRedirect
from django.contrib import admin,messages
from ABC_BizEnrichment.common.helper_function import get_full_function_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from FinalRecords.models import BusinessLocationLicense

# Register your models here.

@admin.register(BusinessLocationLicense)
class BusinessLocationLicenseAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "businesslocationlicense"
    def get_merge_view(self):
        def BusinessLocationLicensemerge_view(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for Filling, Principal & Agent and saved in the Dataset 2 (Combined Information)'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset2record/")  # Redirect after processing
        return BusinessLocationLicensemerge_view
        