from django.urls import path
from django.db.models import F, Func, Value
from django.db.models.functions import Replace, Lower
from merge_data.models import DataSet1Record, DataSet2Record
class CustomMergeAdminMixin:
    """
    Mixin to handle custom merge URL routing and dynamic changelist context.
    """
    merge_url_name = None  # Define in subclasses
    def get_urls(self):
        """
        Add a custom URL for CSV import action.
        """
        urls = super().get_urls()
        custom_urls = [
            path(
                f"{self.merge_url_name}/", 
                 self.get_merge_view(), 
                 name=self.merge_url_name),
        ]
        return custom_urls + urls
    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context[f'{self.merge_url_name}_url'] = f'admin:{self.merge_url_name}'
        return super().changelist_view(request, extra_context=extra_context)
    
    def get_merge_view(self):
        """
        Returns the actual merge view function from the subclass.
        Subclasses should define this method.
        """
        raise NotImplementedError("Subclasses must define 'get_merge_view'.")
    
    def get_merge_view1(self):
        """
        Returns the actual merge view function from the subclass.
        Subclasses should define this method.
        """
        raise NotImplementedError("Subclasses must define 'get_merge_view'.")