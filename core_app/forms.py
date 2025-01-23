# forms.py
from django import forms

class CSVImportForm(forms.Form):
    csv_file = forms.FileField(label="Select CSV File")
