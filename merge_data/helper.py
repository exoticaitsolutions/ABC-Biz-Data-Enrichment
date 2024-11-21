from django.contrib import admin
from django.template.response import TemplateResponse
from django.urls import path
from django.core.exceptions import ObjectDoesNotExist

class CustomMergeAdminMixin:
    """
    Mixin to handle custom merge URL routing and dynamic changelist context.
    """
    merge_url_name = None  # Define in subclasses
    def get_urls(self):
        """
        Add custom URLs for merge actions. The actual view is handled in the subclass.
        """
        urls = super().get_urls()
        if not self.merge_url_name:
            raise ValueError("Subclasses must define 'merge_url_name'")
        custom_urls = [
            path(
                f"{self.merge_url_name}/", 
                self.get_merge_view(),
                name=self.merge_url_name
            ),
        ]
        return custom_urls + urls
    def get_merge_view(self):
        """
        Returns the actual merge view function from the subclass.
        Subclasses should define this method.
        """
        raise NotImplementedError("Subclasses must define 'get_merge_view'.")
    
    def changelist_view(self, request, extra_context=None):
        """
        Add dynamic context to the changelist view.
        """
        extra_context = extra_context or {}
        if not self.merge_url_name:
            raise ValueError("Subclasses must define 'merge_url_name'")
        extra_context[f"{self.merge_url_name}"] = f"admin:{self.merge_url_name}"
        return super().changelist_view(request, extra_context=extra_context)