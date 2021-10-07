import base64
import gzip
import re
from dataclasses import dataclass

from lxml import etree

import config
import utils

etran_request = rf"""
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:sys="SysEtranInt">
<soapenv:Body>
    <sys:GetBlock>
        <Login>{config.etran_login}</Login>
        <Password>{config.etran_password}</Password>
        <Text>{{0}}</Text>
    </sys:GetBlock>
</soapenv:Body>
</soapenv:Envelope>
"""

train_index_pattern1 = re.compile(r'(\d{5})(?:\D)(\d{3})(?:\D)(\d{5})')
train_index_pattern2 = re.compile(r'(?:\d{15})')
xmlns_pattern = re.compile(r'(?:<root.*?>)')  # убираем мусорные namespace'ы


@dataclass
class ETRANResponse:
    is_error: bool
    text: str


def decode_response(response: bytes) -> ETRANResponse:
    is_error, text = False, None
    try:
        # во внутреннем XML ошибочно указывается кодировка windows-1251
        parser = etree.XMLParser(encoding="UTF-8")
        root = etree.fromstring(response, parser)

        # Envelope/Body/GetBlockResponse/Text
        xml = root[0][0].findtext('Text').encode()
        root = etree.fromstring(xml, parser)

        if root.tag == 'error':
            is_error, text = True, root.find('errorMessage').get('value')

        elif root.tag in {'GetInformReply', 'GetInformNSIReply'}:
            if reply := root.findtext('ASOUPReply'):
                xml = reply.encode()
            else:
                reply = root.findtext('ASOUP64Reply').encode()
                xml = gzip.decompress(base64.b64decode(reply))

            # GetInformReply/ASOUPReply/Envelope/Body/getReferenceSPVXXXXResponse/return
            root = etree.fromstring(xml, parser)[0][0][0]
            if root.findtext('returnCode') != '0':
                is_error, text = True, root.findtext('errorMessage')
            else:
                root = root[0]  # referenceSPVXXXX
                root.tag = 'root'
                text = etree.tostring(root, encoding='UTF-8').decode()
                text = xmlns_pattern.sub('<root>', text, 1)

        else:  # getNSIReply, getOrgPassportReply, etc.
            text = etree.tostring(root, encoding='UTF-8').decode()

    except etree.XMLSyntaxError as e:
        is_error, text = True, repr(e)
    finally:
        return ETRANResponse(is_error, text)


def request_SPP4700(query: str) -> str:
    """Работа с поездом"""
    request_template = rf"""
<GetInform>{"<UseGZIPBinary>1</UseGZIPBinary>" if config.etran_gzip else ""}
<ns0:getReferenceSPP4700 xmlns:ns0="http://service.siw.pktbcki.rzd/">
<ns0:ReferenceSPP4700Request>
<idUser>0</idUser>
<indexPoezd>{{0}}</indexPoezd>
</ns0:ReferenceSPP4700Request>
</ns0:getReferenceSPP4700>
</GetInform>
    """
    if m := train_index_pattern1.fullmatch(query):
        train_index = utils.get_code6(
            int(m.group(1))) + m.group(2) + utils.get_code6(int(m.group(3)))
    elif train_index_pattern2.fullmatch(query):
        train_index = query
    else:
        raise ValueError(f'Некорректный формат индекса поезда: {query}')

    return etran_request.format(utils.xml_escape(request_template.format(train_index)))


request_map = {
    1: request_SPP4700
}
