import textract
import sys
import json
import mimetypes
from elasticsearch import Elasticsearch
from elasticsearch import helpers

#큰 제목을 위한것
pdf_title = []
#소제목을 위한것
pdf_small_title = []
#내용을 위한것
pdf_content = []
small_len_for_sentence = 15
es = None #ElasticSearch를 위한것

json_for_pdf = dict()

def check_type(filename):
    if mimetypes.guess_type(sys.argv[1]) == ('application/pdf', None):
        return 1
    else:
        return 0

def check_small_title(small_title):
    for i in range(len(small_title) - 1, -1, -1):
        if i >= 0 and small_title[i] >= '1' and small_title[i] <= '9': #소제목은 거의 1 introduction 이렇게 되어있다.
            if small_title[i + 1] == ' ' and small_title[i + 2] >= 'A' and small_title[i + 2] <= 'Z':
                return 1 #진짜 소제목의 pattern이 맞는지 구별하는 것
            else:
                return 0

def add_small_title():
    pdf_small_title.insert(0, "Abstract")
    pdf_small_title.append('Acknowledgement')
    pdf_small_title.append('References')

def sort_small_title(): #소제목들을 순서대로 정렬하기 위한것.. parsing이 순서대로 되지 않기때문..
    pdf_small_title.sort()

def erase_blank_space(text): #앞부분에 공백을 지우기 위한것
    while text[0] != ' ' and text[0] != '%':
        text = text[1 : len(text)]
    return text

def make_json():
    ft_json = json.dumps(json_for_pdf, indent = 4)
    return ft_json

def textract_for_title(text):
    text = text.decode()
    for_title = False
    tmp_str = ''
    tmp_last_num_index = -1
    for index, text_char in enumerate(text):
        if index - 1 >= 0 and text[index - 1] == '\n' and (text_char >= '1' and text_char <= '9') and for_title == False: #소제목은 거의 1 introduction 이렇게 되어있다.
            for_title = True
        if for_title:
            tmp_str += text_char
            '''
            if text_char != '.' and text_char != ' ' and ((text_char < 'a' or text_char > 'z') and (text_char < 'A' or text_char > 'Z') and (text_char < '1' or text_char > '9')) and text_char != '\n':
                for_title = False
                tmp_str = ''
                continue 
            ''' # 소제목에 어떤 문자가 들어가 있을지 모르므로 일단 뺐다.
            if text_char == '\n':
                tmp_str = tmp_str[0:-1]
                if tmp_str[-1] >= 'a' and tmp_str[-1] <= 'z' and check_small_title(tmp_str) == 1:
                    pdf_small_title.append(tmp_str)
                tmp_str = ''
                for_title = False

def textract_for_big_title(text):
    text = text.decode()

    tmp_str = ''
    text = text.replace('\n', '%')
    #우선 구분을 위해 '\n'을 '%'로 구분한다.
    #제목을 parsing 한다 (\n 과 대문자로 구분).
    #문자을 parsing 한다 (.으로 대문자로 구분).
    for index, text_char in enumerate(text):
        if text_char == '%' and (((text[index + 1] >= 'A' and text[index + 1] <= 'Z') and (text[index - 1] >= 'a' and text[index - 1] <= 'z'))): #Libnvmio : ~~~~~~~ \n Lib
            if len(pdf_title) == 0:
                pdf_title.append(tmp_str) #첫번째 큰제목을 위한 것
                return
            tmp_str = ''
        else:
            if text_char == '%':
                tmp_str += ' '
            else:
                tmp_str += text_char

def textract_split_not_reference(small_title, text, start_index, title_index): #reference가 아닌 문장을 분리한다.
    end_index = text.find(pdf_small_title[title_index + 1])
    text = text.replace('\n', '%') #편의를 위해 \n을 %로 바꾸어준다.
    tmp_str = ''

    json_for_pdf[pdf_title[0]][small_title] = []

    for index in range(start_index, end_index):
        if text[index] == '.' and ((text[index + 2] >= 'A' and text[index + 2] <= 'Z') or (text[index + 1] == '%')):
            tmp_str = erase_blank_space(tmp_str)
            if len(tmp_str) > small_len_for_sentence:
                json_for_pdf[pdf_title[0]][small_title].append(tmp_str)
            tmp_str = ''
        else:
            if text[index] == '%':
                tmp_str += ' '
            else:
                tmp_str += text[index]   


def textract_split_reference(small_title, text, start_index, title_index): #reference 인것은 대게 [25] 형태이므로 이렇게 쓴다.
    end_index = len(text)
    text = text.replace('\n', '%')
    tmp_str = ''

    json_for_pdf[pdf_title[0]][small_title] = []

    for index in range(start_index, end_index):
        if text[index] == '[':
            erase_blank_space(tmp_str)
            json_for_pdf[pdf_title[0]][small_title].append(tmp_str)
            tmp_str = ''
            tmp_str += text[index]
        else:
            if text[index] == '%':
                tmp_str += ' '
            else:
                tmp_str += text[index]
    
    json_for_pdf[pdf_title[0]][small_title].append(tmp_str) # [을 기준으로 하였기 때문에 마지막으로 처리 못한 str을 붙이는것

def connect_to_elastic():
    es = Elasticsearch('localhost:9200')

def make_index(index_name):
    if es.indices.exists(index = index_name):
        es.indices.delete(index = index_name)
    es.indices.create(index = index_name)

def searchAPI(index_name, query):
    res = es.search(index = index_name, body = query) #query를 던진다.
    return res

if __name__ == '__main__':
    if check_type(sys.argv[1]):
        text = textract.process(sys.argv[1]) #텍스트로 바꾸기
        textract_for_title(text)
        sort_small_title()
        add_small_title()
        textract_for_big_title(text)
        json_for_pdf[pdf_title[0]] = dict() #큰제목이 가장 큰 index가 되어진다.
        text = text.decode()
        for i in range(len(pdf_small_title)):
            if pdf_small_title[i] != 'References':
                textract_split_not_reference(pdf_small_title[i], text, text.find(pdf_small_title[i]) + len(pdf_small_title[i]), i)
            else:
                textract_split_reference(pdf_small_title[i], text, text.find(pdf_small_title[i]) + len(pdf_small_title[i]), i)
        
        #여기서 부터는 elastic-search에 넣기 위한 코드이다.
        index_name = pdf_title[0]
        connect_to_elastic()
        make_index(index_name)

        es.index(index = index_name, doc_type = 'example1', body = make_json(), indent = 4)
        ex.indices.refresh(index = index_name)

    else:
        print('Not PDF Type')
        sys.exit(1)
    