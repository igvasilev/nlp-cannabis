import datetime
import logging
import re
from abc import abstractmethod

from bs4 import BeautifulSoup
from lxml import etree


class BaseParser:

    source_name: str

    @classmethod
    @abstractmethod
    def parse_file(cls, f):
        doc_obj = None
        yield doc_obj

    @classmethod
    def get_parsers_classes(cls):
        return (subclass for subclass in cls.__subclasses__())


class PubMedXMLParser(BaseParser):

    _empty_brackets_re = re.compile(r'(\s*?\[[ \-–,;]*?\])|(\s+?\([ \-–,;]*?\))')
    _spaces_with_delimeter_re = re.compile(r'(\s+[-,;]\s+?)+')
    _whitespaces_re = re.compile(r'\s{2,}')

    source_name = 'pubmed'
    author_name_parse_map = {
        'ForeName': 'first_name',
        'LastName': 'last_name',
        'Initials': 'initials',
        'Suffix': 'suffix'
    }
    
    @classmethod
    def parse_file(cls, xml_string: str):
        xml_pubmed_article_set = etree.fromstring(xml_string)
        for pubmed_article_el in xml_pubmed_article_set.findall('PubmedArticle'):
            article_data = cls._get_abstract_data_from_xml_el(pubmed_article_el)
            if article_data is not None:
                yield article_data

    @classmethod
    def _get_authors(cls, article_el):
        authors = []
        collective_name = None
        author_list_el = article_el.find('AuthorList')
        if author_list_el is not None:
            for author_el in author_list_el.findall('Author'):
                author = {}
                for name_el in author_el:
                    if name_el.tag in cls.author_name_parse_map:
                        author[cls.author_name_parse_map[name_el.tag]] = name_el.text
                    elif name_el.tag == 'CollectiveName':
                        collective_name = name_el.text
                if author:
                    authors.append(author)
        return authors, collective_name

    @classmethod
    def _get_date(cls, medline_citation_el, key='DateCompleted'):
        date_el = medline_citation_el.find(key)
        if date_el is not None:
            year = date_el.find('Year').text
            month = date_el.find('Month').text
            day = date_el.find('Day').text
            return datetime.datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d')

    @classmethod
    def _clean_text(cls, text):
        soup_text = BeautifulSoup(text, 'html.parser')
        # remove <xref> and <a> links
        for link in soup_text.find_all(["xref", "a"]):
            link.extract()
        # replace <sub>VAL</sub> or <sup>VAL</sup> on -VAL
        for sub_or_sup in soup_text.find_all(["sub", "sup"]):
            sub_or_sup.replace_with(f'-{sub_or_sup.text}')
        # remove all other tags
        text = soup_text.get_text()
        # remove encoded characters
        text = cls._decode_unicode_references(text)        
        # remove uninformative [] () brackets and reduce spaces
        text = cls._empty_brackets_re.sub('', text)
        text = cls._spaces_with_delimeter_re.sub(' ', text)
        text = cls._whitespaces_re.sub(' ', text)
        return text.strip()

    @classmethod
    def _get_texts(cls, article_el):
        abstract_el = article_el.find('Abstract')
        texts = []
        if abstract_el is not None:
            for abstract_text_el in abstract_el.findall('AbstractText'):
                abstract_text = etree.tostring(abstract_text_el).decode('utf-8')
                abstract_text = cls._clean_text(abstract_text)
                if abstract_text:
                    texts.append({
                        'text': abstract_text,
                    })
                    tags = dict(abstract_text_el.attrib)
                    if 'NlmCategory' in tags:
                        texts[-1]['nlm_category'] = tags['NlmCategory']
        return texts

    @classmethod
    def _decode_unicode_references(cls, text):
        def _callback(matches):
            id = matches.group(1)
            try:
                return chr(int(id))
            except:
                return id
        return re.sub(r"&#(\d+)(;|(?=\s))", _callback, text)

    @classmethod
    def _is_review(cls, medline_citation_el):        
        article_el = medline_citation_el.find('Article')
        pub_type_l_el = article_el.find('PublicationTypeList')
        # is review as document category?
        if pub_type_l_el is not None:
            for pub_type_el in pub_type_l_el.findall('PublicationType'):
                if pub_type_el.text is not None and 'review' in pub_type_el.text.lower():
                    return True
        # is review at MESH categories?
        mesh_heading_list_el = medline_citation_el.find('MeshHeadingList')
        if mesh_heading_list_el is not None:
            for mesh_heading_el in mesh_heading_list_el.findall('MeshHeading'):
                descriptor_name_el = mesh_heading_el.find('DescriptorName')
                if descriptor_name_el.text is None:
                    continue
                mesh_name_lower = descriptor_name_el.text.lower()
                if 'review' in mesh_name_lower:
                    return True
        # is review at keywords?
        keyword_list_el = medline_citation_el.find('KeywordList')
        if keyword_list_el is not None:
            for keyword_el in keyword_list_el.findall('Keyword'):
                if keyword_el.text is not None and 'review' in keyword_el.text.lower(): 
                    return True
        # is not any above - return False
        return False

    @classmethod
    def _is_review_in_text(cls, text):
        # if any of phrases below present in text - article is a review
        filtering_phrases = (
            'systematic review',
        )
        text_lower = text.lower()
        for filtering_phrase in filtering_phrases:
            if filtering_phrase in text_lower:
                return True
        # is not any above - return False
        return False

    @classmethod
    def _get_abstract_data_from_xml_el(cls, pubmed_article_el):
        article_data_dict = {}
        medline_citation_el = pubmed_article_el.find('MedlineCitation')
        pmid_el = medline_citation_el.find('PMID')
        if pmid_el is None:
            return None
        article_data_dict['pmid'] = pmid_el.text
        article_data_dict['id'] = f"{cls.source_name}_{article_data_dict['pmid']}"

        if cls._is_review(medline_citation_el):
            return None

        article_el = medline_citation_el.find('Article')
        texts = cls._get_texts(article_el)
        if texts:
            article_data_dict['abstract_texts'] = texts

        try:
            article_data_dict['date'] = article_el.find('Journal/JournalIssue/PubDate/Year').text
        except AttributeError:
            article_data_dict['date'] = article_el.find('Journal/JournalIssue/PubDate/MedlineDate').text[:4]

        article_data_dict['journal_title'] = article_el.find('Journal/Title').text  

        article_title_el = article_el.find('ArticleTitle')
        if article_title_el is not None:
            article_title = etree.tostring(article_title_el).decode('utf-8')
            article_title = cls._clean_text(article_title)
            if article_title:
                article_data_dict['title'] = article_title

        if not ('title' in article_data_dict or 'abstract_texts' in article_data_dict):
            return None

        text = f"{article_data_dict.get('title', '')}\n{article_data_dict.get('abstract_texts', '')}".strip()
        if cls._is_review_in_text(text):
            return None

        authors, collective_name = cls._get_authors(article_el)
        if authors:
            article_data_dict['authors'] = authors
        if collective_name is not None:
            article_data_dict['collective_name'] = collective_name

        # todo: extract the date of publication
        article_data_dict['date_completed'] = cls._get_date(medline_citation_el, key='DateCompleted')
        article_data_dict['date_revised'] = cls._get_date(medline_citation_el, key='DateRevised')
        article_data_dict['date_article'] = cls._get_date(article_el, key='ArticleDate')

        # keywords = []
        # keyword_list_el = medline_citation_el.find('KeywordList')
        # if keyword_list_el is not None:
        #     keywords = [keyword_el.text for keyword_el in keyword_list_el.findall('Keyword')]
        # article_data_dict['keywords'] = keywords
        return article_data_dict
