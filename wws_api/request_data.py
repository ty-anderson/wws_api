import html
import asyncio
import aiohttp
from lxml import etree


def request_wws(url, username, password, xml_payload) -> list:
    """
    Main function to pull data from Workday Web Services API. Requires an XML template that is specific to the API call.
    The ISU account must also be set up to use the specific API. To add a new template to this package,
    save the XML file to wws/async_request/templates.

    Notes:

    This is the highest level function to call for WWS data retrieval. In most cases this is the only function you will need to call.
    This function calls the xml_template function to create the template and url for the API call. Then it calls the \
    async_requests function to make the API call.

    Examples:

    .. code-block:: python

        responses = wws.request_wws(url='', payload='')

    """
    envelope = create_payload(username=username, password=password, xml_body=xml_payload)
    return asyncio.run(_generate_requests(url, envelope))


def create_payload(username: str, password: str, xml_body: str) -> str:
    """
    Create the full payload for the API call, wraps the request body in XML envelope with creds.

    :param username: ISU_USERNAME
    :param password: ISU_PASSWORD
    :param xml_body: Prepared XML payload request.
    :return: Full payload for API call.
    """
    escaped_password = escape_html(password)
    return f"""
    <?xml version="1.0" encoding="utf-8"?>
    <env:Envelope
            xmlns:env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
        <env:Header>
            <wsse:Security env:mustUnderstand="1">
                <wsse:UsernameToken>
                    <wsse:Username>{username}</wsse:Username>
                    <wsse:Password
                            Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">
                        {escaped_password}
                    </wsse:Password>
                </wsse:UsernameToken>
            </wsse:Security>
        </env:Header>
        <env:Body>
            {xml_body}
        </env:Body>
    </env:Envelope>
    """.strip()


def escape_html(text):
    """Encode special characters (<, >, &, etc.) to HTML-safe sequences."""
    return html.escape(text)


async def _generate_requests(url: str, xml_template: str) -> list:
    """
    Call Workday Web Services API.

    :param url:
    :param xml_template:
    :return: list of xml responses.

    Example:

        .. code-example:: python

    """
    payload = xml_template.replace('{{ page }}', str(1))
    payload = payload.replace('{ page }', str(1))

    # GET TOTAL PAGES ON THIS API CALL
    timeout = aiohttp.ClientTimeout(total=8000)
    async with aiohttp.ClientSession(timeout=timeout) as session:

        response = await _hit_wws(session=session, url=url, payload=payload)
        try:
            pages = int(
                etree.fromstring(response).find('.//wd:Total_Pages', namespaces={'wd': 'urn:com.workday/bsvc'}).text)

        except AttributeError:
            msg = f'Exiting gracefully - Error finding pages in API call. Response: {str(response)[:2000]}'
            raise ValueError(msg)

        print(f'Number of pages: {pages}')

        if pages > 8000:
            msg = f'Too many pages to process. Please narrow your date range. Pages: {pages}, should be less that 8000'
            raise ValueError(msg)

        tasks = []
        for number in range(1, pages + 1):
            payload = xml_template.replace('{{ page }}', str(number))
            payload = payload.replace('{ page }', str(number))
            tasks.append(asyncio.ensure_future(_hit_wws(session, url, payload)))

        web_calls = await asyncio.gather(*tasks)
        return web_calls


async def _hit_wws(session: aiohttp.ClientSession, url: str, payload: str):
    """
    Async function that hits the WWS API.

    :param session:
    :param url:
    :param payload:
    :return:
    """
    async with session.post(url, headers={'Content-Type': 'application/xml'}, data=payload) as resp:
        return await resp.read()
