import re
import json
import xmltodict
from lxml import etree
import pyarrow as pa


def to_pyarrow(responses: list | tuple, start_tag: str, tags: list, allow_collections: bool = False) -> pa.Table:
    """
    High-level function to extract data from xml responses.
    Combine all xml responses into one list starting from start_tag. Extract tags from xml and load to pyarrow table..

    :param responses: list, list of xml responses from :py:func:`request_wws <pacs_data_etl.api.workday.wws.request_wws>`.
    :param start_tag: str, starting tag to search for in xml. Should be the tag closest to the data you want to extract.
    :param tags: list, list of tags nested in start tag to extract from xml.
    :param allow_collections: allow xml values to be parsed into lists where number of values is greater than one.
    :return: pyarrow Table with data extracted from xml responses..

    **Tags Notes:**

    - Use ``'>>'`` to pull a data point that is nested. ie ``'Journal_Entry_Line_Data>>Memo''``.
    - Use ``'*'`` to go a level deeper where there is multiple nested data with the same tag.
    - Use ``'~'`` to grab all elements under a parent node.
    - Use ``'^^'`` at the end of an element to set the column name; ie ``'Journal_Entry_Line_Data>>Memo^^Journal_Memo'``.
    - Use ``'@@'`` to pull an attribute from an element; ie ``'@@Primary_Job'``. Currently only works if at root level, can't do ``'Worker_Data>>@@Primary_Job'``.
    - Use ``'||'`` to pull an alternative element if the first one is null; Must contain an ``'|='`` after last OR \
    statement. ie ``"Parent_Element>>ID[@wd:type='Try_First_Value']||Parent_Element>>ID[@wd:type='Try_Second_Value']|=COLUMN_RENAME_VALUE_TYPE"``. \
    Able to chain this as many times as needed. It is also recommended to use ``'^^'`` To rename the column containing the element values (MUST BE LAST ARGUMENT).
    - Use ``'|='`` to rename a ``'||'`` column. Only used for OR statements.
    - Use ``'%'`` as a wild card search function. Must contain a starting tag and end tag as well as a ``'?='`` (type) tag (see example below). \
    ie. ``'%start?=tag%'`` (searches for a tag containing "start" such as "Start_Date")

        #. ``%`` Starts the search value.
        #. ``start`` Value to search for.
        #. ``?=`` Tells the function what type of element to find.

            - Currently works with (``'tag'``, ``'type'``, ``'text'``)

        #. ``tag`` The element type.
        #. ``%`` Ends the search function.

    **Examples:**

    Simple example, get gl accounts

    .. code-block:: python

        from pacs_data_etl.api.workday import wws

        responses = wws.request_wws(template_name='get_account_sets')

        df = wws.extract_data(responses=responses,
                              start_tag='Ledger_Account_Data',
                              tags=['Ledger_Account_Identifier',
                                    'Ledger_Account_Name',
                                    'Retired',
                                    "Ledger_Account_Type_Reference>>ID[wd:@type='Ledger_Account_Type_ID']"
                                    ]
                              )

    Lets look at another example with a little more code, get ledger account summaries:

    .. code-block:: python

        from pacs_data_etl.api.workday import wws

        responses = wws.request_wws(template_name='get_ledger_account_summaries')

        df = wws.extract_data(responses=responses,
                      start_tag='Ledger_Account_Summary',
                      tags=['Ledger_Account_Summary_Data>>Ledger_Account_Summary_ID',
                            'Ledger_Account_Summary_Data>>Ledger_Account_Summary_Name',
                            "*Ledger_Account_Summary_Data",  # get nested data that has multiple elements with same name
                            "Included_Ledger_Account_Reference>>ID[@wd:type='Ledger_Account_ID']"
                            ]
                      )

    More complex example, get journals:

    .. code-block:: python

        import datetime
        from pacs_data_etl.api.workday import wws


        def main(date_type, start_date, end_date):
            responses = wws.request_wws(template_name='get_journals',
                                        from_date=start_date,
                                        to_date=end_date,
                                        date_type=date_type
                                        )

            df = wws.extract_data(responses=responses,
                                  start_tag='Journal_Entry_Data',
                                  tags=['Journal_Number',
                                        'Accounting_Date',
                                        "Journal_Source_Reference>>ID[@wd:type='Journal_Source_ID']",
                                        '*Journal_Entry_Line_Data',  # get nested data in this tag
                                        'Debit_Amount',
                                        'Credit_Amount',
                                        'Memo',
                                        "Line_Company_Reference>>ID[@wd:type='Company_Reference_ID']",
                                        "Ledger_Account_Reference>>ID[@wd:type='Ledger_Account_ID']",
                                        "Worktags_Reference>>ID[@wd:type='Spend_Category_ID']",
                                        "Worktags_Reference>>ID[@wd:type='Cost_Center_Reference_ID']",
                                        "Worktags_Reference>>ID[@wd:type='Supplier_Reference_ID']",
                                        "Worktags_Reference>>ID[@wd:type='Supplier_ID']",
                                        "Worktags_Reference>>ID[@wd:type='Employee_ID']",
                                        "Worktags_Reference>>ID[@wd:type='Earning_Code']",
                                        "Worktags_Reference>>ID[@wd:type='Deduction_Code']",
                                        "Worktags_Reference>>ID[@wd:type='Workday_Deduction_Code']",
                                        "Worktags_Reference>>ID[@wd:type='Expense_Item_ID']",
                                        "Worktags_Reference>>ID[@wd:type='Revenue_Category_ID']^^Employee_ID",
                                        "Worktags_Reference>>ID[@wd:type='Custom_Worktag_4_ID']^^Pay_Component_ID",
                                        "Worktags_Reference>>ID[@wd:type='Custom_Worktag_5_ID']",
                                        "Worktags_Reference>>ID[@wd:type='Custom_Worktag_06_ID']"]
                                  )

            # Use dataframe here...

        if __name__ == '__main__':
            date_type = 'updated_date'
            end = datetime.datetime(2023, 12, 25)
            start = end - datetime.timedelta(hours=12)

            match date_type:
                case 'accounting_date':
                    # PULL ALL JOURNAL ENTRIES BASED ON ACCOUNTING DATE
                    main(date_type='accounting_date', start_date=start, end_date=end, overwrite=True)
                case 'updated_date':
                    # PULL ALL JOURNAL ENTRIES UPDATED IN THE DATE RANGE
                    main(date_type='updated_date', start_date=start, end_date=end)
                case _:
                    raise ValueError('date_type is not correct!')

    """
    # CHECK IF RESPONSES IS EMPTY
    if len(responses) < 1:
        raise ValueError('No responses returned from API')

    # CONVERT XML BYTES TO XML TREE AND COMBINE INTO ONE LIST
    list_of_xml_tree = [etree.fromstring(xml).findall('.//wd:' + start_tag, namespaces={'wd': 'urn:com.workday/bsvc'})
                        for xml in responses]
    xml_list_by_start_tag = [xml for xml_tree in list_of_xml_tree for xml in xml_tree]

    # XML PARSING AND EXTRACTION
    tags = ['./wd:' + tag.replace('>>', '/wd:') for tag in tags]
    tags = [tag.replace('||', '||./wd:') for tag in tags]
    ns = {'wd': 'urn:com.workday/bsvc'}
    data = _pull_data(responses=xml_list_by_start_tag, tags=tags, ns=ns, allow_collections=allow_collections)

    # CONVERT TO DATAFRAME AND RENAME COLUMNS
    table = pa.Table.from_pylist(data)

    # Rename logic
    col_rename = {}
    for col in table.column_names:
        match = re.search(r"=(.*?)\]", col)
        if match:
            col_rename[col] = match.group(1).replace("'", '')
        elif '/wd:' in col:
            col_rename[col] = col.split('/wd:')[-1]

    # Apply renaming
    new_columns = [col_rename.get(name, name) for name in table.column_names]
    renamed_table = table.rename_columns(new_columns)
    return renamed_table


def to_dict(responses: tuple | list, max_num: int = None) -> list:
    """
    Convert xml responses to a list of dictionaries. Slow with large datasets but useful for exploration of data.

    :param responses: list, list of responses from :py:func:`request_wws <pacs_data_etl.api.workday.wws.request_wws>`.
    :param max_num: int, maximum number of responses to convert to dictionary.
    :return: list, list of dictionaries

    Example

    .. code-block:: python

        responses = wws.request_wws(template_name='get_journals')
        dicts = wws.to_dict(responses=responses)

    """
    dicts = []
    for i, xml_string in enumerate(responses):
        if max_num and i >= max_num:
            break
        dicts.append(xmltodict.parse(xml_string))

    return dicts


def to_json(responses: tuple, file_name: str, max_num: int = None) -> None:
    """
    Save responses to a json file(s) for exploration. Will save one file per response.
    Use max_num to limit the number of responses saved.

    :param responses: list, list of responses from :py:func:`request_wws <pacs_data_etl.api.workday.wws.request_wws>`.
    :param file_name: str, name of json file. ie. 'get_journals'
    :param max_num: int, maximum number of responses to save to json file

    Example

    .. code-block:: python

        responses = wws.request_wws(template_name='get_journals')
        wws.to_json(responses, file_name='get_journals')

    .. code-block:: python

        responses = wws.request_wws(template_name='get_journals')
        wws.to_json(responses, file_name='get_journals', max_num=3)

    """
    dicts = to_dict(responses=responses, max_num=max_num)

    for i, d in enumerate(dicts):
        json_string = json.dumps(d, indent=4)
        with open(f'{file_name}_{i}.json', 'w') as f:
            f.write(json_string)

        print(f'Saved {file_name}')


def _pull_data(responses: list, tags: list, ns: dict, high_level_tags: dict = None, allow_collections: bool = False) -> list:
    """
    Pull and organize XML data into a list of dictionaries to be converted to DataFrame.

    #. Loop through all elements in API responses.
    #. In each loop, check the element for existence of each tag parameter.

    :param responses: list, list of xml responses
    :param tags: list, list of tags to pull from xml
    :param ns: dict, namespace for xml
    :param high_level_tags: dict, dictionary of high level tags to add to each row
    :param allow_collections: allow xml values to be parsed into lists where number of values is greater than one
    :return: list, list of dictionaries with data from xml
    """
    row_list = []
    # LOOP THROUGH XML LIST
    for element in responses:
        # HIGH LEVEL TAGS USED FOR RECURSION
        added_to_list = False
        if high_level_tags is not None:
            row_dict = high_level_tags.copy()
        else:
            row_dict = {}
        # PULL EACH TAG FROM XML ELEMENT
        for tag in tags:
            # IF MARKED WITH '*' THEN FINDALL TO GO A LEVEL DEEPER AND GO TO NEXT TAG TO SEARCH ELEMENTS
            if '*' in tag:
                elements = element.findall('./' + tag.replace('*', ''), namespaces=ns)
                nested_tags = _next_tags(tags=tags, curr_tag=tag)
                sub_list = pull_data(responses=elements,
                                     tags=nested_tags, ns=ns,
                                     high_level_tags=row_dict,
                                     allow_collections=allow_collections)
                row_list.extend(sub_list)
                added_to_list = True
                break
            elif '~' in tag:
                parent_path = tag.replace('~', '').strip()
                elems = element.xpath(parent_path, namespaces=ns)
                if len(elems) == 1:
                    elem = elems[0]
                    # Use recursive function to extract data without parent prefix
                    nested_data = _extract_element_data(elem, ns, prefix='')  # Set prefix to empty string
                    row_dict.update(nested_data)
                elif len(elems) > 1:
                    if allow_collections:
                        collected_data = {}
                        for e in elems:
                            nested_data = _extract_element_data(e, ns, prefix='')  # Set prefix to empty string
                            for k, v in nested_data.items():
                                if k in collected_data:
                                    collected_data[k].append(v)
                                else:
                                    collected_data[k] = [v]
                        row_dict.update(collected_data)
                    else:
                        sub_list = []
                        for e in elems:
                            row_d = row_dict.copy()
                            nested_data = _extract_element_data(e, ns, prefix='')  # Set prefix to empty string
                            row_d.update(nested_data)
                            sub_list.append(row_d)
                        row_list.extend(sub_list)
                        added_to_list = True
                else:
                    # No elements found
                    row_dict[tag] = None
                added_to_list = True
                continue  # Proceed to next tag
            else:
                # PULL TAG FROM XML ELEMENT - SHOULD ONLY BE ONE ELEMENT.
                # RENAME TAG IF MARKED WITH '^^'
                if '^^' in tag:
                    tag, name = tag.split('^^')
                else:
                    name = tag

                # CHECK IF ATTRIBUTE
                # ie Company_Data>>Contact_Data>>Address_Data>>@@Formatted_Address
                # to capture data structure end point {'Address Data': '@wd:Formatted_Address'}
                if '@@' in tag:
                    tmp_tag = tag.replace('wd:@@', '@wd:')
                    tag = tag.replace('@@', '')
                    t = f"{{{ns.get('wd')}}}" + tag.replace('./wd:', '')
                    elem = element.get(t)
                    if elem is None:
                        elem = element.xpath(tmp_tag, namespaces=ns)
                        row_dict.update({name: elem[0] if elem else None})
                    else:
                        row_dict.update({name: elem})

                    continue

                elem = None



                # Checks if the tag contains a wildcard.
                if '%' in tag:
                    xpath = []
                    # splits it into multiple parts if an OR Statement is found.
                    multi_tags = tag.split('||')
                    for i in multi_tags:
                        try:
                            # Get the variables needed for the search function.
                            xpath_to_search, search_part, end_of_path = i.split('%')
                            search_term, data_type = search_part.split('?=')
                            xpath_to_search = xpath_to_search.strip()
                            search_term = search_term.strip()
                            data_type = data_type.strip()
                            end_of_path = end_of_path.strip()
                        except ValueError as e:
                            print(f"Error processing tag '{i}': {e}")
                            continue

                        # Different elements to search for.
                        if data_type == 'type':
                            xpath_expression = f"{xpath_to_search}ID[contains(translate(@wd:type, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{search_term.lower()}')]{end_of_path}"
                            xpath.append(xpath_expression)
                        elif data_type == 'text':
                            xpath_expression = f"{xpath_to_search}ID[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{search_term.lower()}')]{end_of_path}"
                            xpath.append(xpath_expression)
                        elif data_type == 'tag':
                            xpath_expression = f"{xpath_to_search}*[contains(translate(local-name(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{search_term.lower()}')]{end_of_path}"
                            xpath.append(xpath_expression)
                        else:
                            print(f"Unknown data_type '{data_type}' in tag '{i}'")
                            continue
                    tag = '||'.join(xpath) # Joins the tag back if there were more than one.


                # Checks if there is an OR Clause in the tag
                # Get element via xpath
                if '||' in tag:
                    typename = tag.split('|=') # Grabs column name where it will store the value type
                    tag = tag.replace('|=' + typename[1], '') # formats the tag without the rename
                    for i in tag.split('||'):
                        elem = element.xpath(i, namespaces=ns)
                        if len(elem) == 1:
                            elem_name = elem[0].get(f'{{{ns.get("wd")}}}type')
                            row_dict.update({typename[1]: elem_name})
                            break
                else:
                    # If no OR clause grab default tag.
                    elem = element.xpath(tag, namespaces=ns)

                # ADD ELEMENT TO ROW_DICT
                if len(elem) == 1:
                    if hasattr(elem[0], 'text'):
                        row_dict.update({name: elem[0].text})
                    else:
                        row_dict.update({name: elem[0]})


                # if there is more than one element
                elif len(elem) > 1:
                    if allow_collections:  # lists are allowed in one cell instead of trying to make new rows
                        sub_list = []
                        for e in elem:
                            if hasattr(elem[0], 'text'):
                                sub_list.append(e.text)
                            else:
                                sub_list.append(e)

                        row_dict[name] = sub_list
                    else:
                        sub_list = []
                        for e in elem:
                            row_d = row_dict.copy()
                            row_d.update({name: e.text})
                            sub_list.append(row_d)
                        row_list.extend(sub_list)
                        added_to_list = True

        if not added_to_list:
            row_list.append(row_dict)

    return row_list


def _next_tags(tags: list, curr_tag: str) -> list:
    """
    Get next tags to search for in xml. Used for nested elements.
    """
    activated = False
    tmp_tags = []
    for tmp_tag in tags:
        if activated:
            tmp_tags.append(tmp_tag)
        if tmp_tag == curr_tag and not activated:
            activated = True
    return tmp_tags


def _extract_element_data(element, ns, prefix='', max_length=63, existing_keys=None):
    """
    Recursively extract text values from an XML element and its children.

    :param element: The XML element to extract data from.
    :param ns: Namespace dictionary for XML parsing.
    :param prefix: A string prefix for key names to reflect hierarchy.
    :param max_length: Maximum length for keys (e.g., 63 for PostgreSQL).
    :param existing_keys: A set to keep track of existing keys to ensure uniqueness.
    :return: A dictionary with extracted data.
    """
    if existing_keys is None:
        existing_keys = set()
    data = {}
    for child in element:
        tag_name = etree.QName(child.tag).localname
        key = f"{prefix}_{tag_name}" if prefix else tag_name

        # Shorten the key if necessary
        original_key = key
        key = _shorten_column_name(key, max_length=max_length)

        existing_keys.add(key)

        # Extract attributes
        for attr_name, attr_value in child.attrib.items():
            attr_local_name = etree.QName(attr_name).localname
            attr_key = f"{key}_{attr_local_name}"
            # Shorten the attribute key if necessary
            attr_key = _shorten_column_name(attr_key, max_length=max_length)
            existing_keys.add(attr_key)
            data[attr_key] = attr_value

        if len(child):  # If the child has its own children
            # Recursively extract data
            child_data = _extract_element_data(child, ns, prefix=key, max_length=max_length, existing_keys=existing_keys)
            data.update(child_data)
        else:
            text_value = child.text
            data[key] = text_value
    return data

def _shorten_column_name(col, max_length=60):
    if len(col) <= max_length:
        return col

    # Calculate the index where the last max_length characters start
    start_index = len(col) - max_length

    # Try to find the first underscore at or after start_index
    next_sep_index = col.find('_', start_index)
    if next_sep_index != -1 and len(col) - next_sep_index - 1 <= max_length:
        # Found an underscore; use the substring after it
        new_col = col[next_sep_index + 1:]
    else:
        # No underscore after start_index or result too long
        # Try to find the last underscore before start_index
        prev_sep_index = col.rfind('_', 0, start_index)
        if prev_sep_index != -1 and len(col) - prev_sep_index - 1 <= max_length:
            # Found an underscore. use the substring after it
            new_col = col[prev_sep_index + 1:]
        else:
            # No underscores found. take the last max_length characters
            new_col = col[-max_length:]
    return new_col

