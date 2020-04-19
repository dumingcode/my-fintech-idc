import re


def clearHtmlRe(src_html):
    '''
        正则清除HTML标签
        :param src_html:原文本
        :return: 清除后的文本
    '''
    content = re.sub(r"</?(.+?)>", "", src_html)  # 去除标签
    dst_html = re.sub(r"\s+", "", content)  # 去除空白字符
    return dst_html
