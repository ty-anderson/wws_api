# WWS API

The Workday Web Services API is a powerful option to extract data from the Workday software.
It is a SOAP API that requires requests and responses to be in XML format and are deeply nested structures.
This package aims to simplify the process of building scripts that use the API.

There are only a few main functions you'll need to get started:
```python
import wws_api

# pull from API
responses = wws_api.request_wws(url, username, password, xml_template)

# options to process the responses
wws_api.to_dict(responses)
wws_api.to_json(responses)
wws_api.to_pyarrow(responses, start_tag, tags)
```

Here is a full example of how to extract all companies and options to process the response data.
```python
import wws_api

# create your XML payload template (use WWS documentation).
xml_template = """
    <bsvc:Get_Workday_Companies_Request xmlns:bsvc="urn:com.workday/bsvc" bsvc:version="v44.0">
            <bsvc:Response_Filter>
                <bsvc:Page>{{ page }}</bsvc:Page>
            </bsvc:Response_Filter>
            <bsvc:Response_Group>
                <bsvc:OX_Only>false</bsvc:OX_Only>
            </bsvc:Response_Group>
        </bsvc:Get_Workday_Companies_Request>
    """

# make the request
responses = wws_api.request_wws(url='https://services1.myworkday.com/ccx/service/pacs/Financial_Management/v44.0',
                                username='username',
                                password="password",
                                xml_payload=xml_template)

# options to format the response into a more usable format
research = wws_api.to_dict(responses)  # converts nested xml to dict
wws_api.to_json(responses, file_name='companies', max_num=1)  # saves xml data to JSON file.

# if you want to load to your dataframe of choice (pandas, polars, DuckDB, etc.) load to pyarrow
# and then you can use the built-in methods to convert.
pyarrow_table = wws_api.to_pyarrow(
    responses=responses,
    start_tag='Company',
    tags=["Company_Reference>>ID[@wd:type='Company_Reference_ID']",
          'Company_Data>>Tax_ID_Data>>Tax_ID_Text^^Tax_ID',
          "Company_Data>>Tax_ID_Data>>Tax_ID_Type_Reference>>ID[@wd:type='Tax_ID_Type']",
          'Company_Data>>Organization_Data>>ID^^Organization_Reference_ID',
          'Company_Data>>Organization_Data>>Organization_Name',
          'Company_Data>>Organization_Data>>Organization_Code',
          'Company_Data>>Organization_Data>>Organization_Active',
          "Company_Data>>Organization_Subtype_Reference>>ID[@wd:type='Organization_Subtype_ID']",
          'Company_Data>>Contact_Data>>Address_Data>>@@Formatted_Address^^Full_Address',
          "Company_Data>>Contact_Data>>Address_Data>>Address_Line_Data[@wd:Type='ADDRESS_LINE_1']^^address_line_one",
          "Company_Data>>Contact_Data>>Address_Data>>Address_Line_Data[@wd:Type='ADDRESS_LINE_2']^^address_line_two",
          'Company_Data>>Contact_Data>>Address_Data>>Municipality',
          'Company_Data>>Contact_Data>>Address_Data>>Country_Region_Descriptor',
          'Company_Data>>Contact_Data>>Address_Data>>Postal_Code'
          ]
)

# convert to pandas
df = pyarrow_table.to_pandas()

# convert to polars
import polars as pl
pd_df = pl.from_arrow(pyarrow_table)

# write to parquet
import pyarrow.parquet as pq
pq.write_table(pyarrow_table, 'file_name.parquet')


```


## Other Notes

- Passwords are automatically html escaped, no need to process before hand.
- Clean up is done on the xml template to prevent request failures based on whitespace.
