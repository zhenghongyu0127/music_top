import requests
from lxml import etree
import re
import json
import pymysql
import time
sql_client = pymysql.connect(host = '127.0.0.1',user = 'root',password = '',db = 'qq_ceshi_db',charset = 'utf8')
cur = sql_client.cursor()
# import pymongo
# m = pymongo.MongoClient('127.0.0.1')['QQ_music_top20']['qq_music']
#  test 
#test
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'}

# 获取各排行榜url
def get_top_urllist(url):
    html = requests.get(url,headers = headers).text
    url_list = etree.HTML(html).xpath('//div[@class="toplist_nav"]//dd/a/@href')
    for detail_url in url_list:
        yield detail_url

# 获取各排行榜下歌曲详情url以及榜单名字和时间
def get_top_msg(top_id):
    top_url = 'https://u.y.qq.com/cgi-bin/musicu.fcg?data={%22detail%22:{%22module%22:%22musicToplist.ToplistInfoServer%22,%22method%22:%22GetDetail%22,%22param%22:{%22topId%22:'+top_id+',%22offset%22:0,%22num%22:1000}},%22comm%22:{%22ct%22:24,%22cv%22:0}}'
    top_html = requests.get(top_url,headers = headers).text

    parse_top(top_html)

# 解析json文件 获取top信息
def parse_top(top_html):
    top_json = json.loads(top_html)
    # 分类名
    top_title = top_json['detail']['data']['data']['title']
    print('正在下载'+top_title+'信息')
    # 更新时间
    top_time = top_json['detail']['data']['data']['updateTime']

    song_list = top_json['detail']['data']['data']['song']

    song_detail = top_json['detail']['data']['songInfoList']
    for j in song_detail:
        song_name = j['name'] #歌曲名称
        song_id = j['id'] #歌曲ID
        song_mid = j['mid'] #歌曲mid
        song_long_time = j['interval'] # 歌曲时长/秒
        song_time_public = j['album']['time_public'] # 歌曲发行时间

        create_time = time.strftime('%Y-%m-%d-%H',time.localtime(time.time()))

        singer_list = []
        for singer_ms in j['singer']:
            singer_msg = singer_ms['mid']+'_'+str(singer_ms['id'])+'_'+singer_ms['name']
            singer_list.append(singer_msg)

        singer_str = ','.join(singer_list)

        company_name, company_id, genre_name, language, album_name, album_id, singer = get_music_detail(song_id, song_mid)
        # print(company_name,genre_name,language,album,singer)

        try:
            sql = 'insert into `qq_music_detail`(`top_title`,`top_time`,`song_name`,`song_id`,`song_mid`,`song_long_time`,`song_time_public`,`company_name`,`company_id`,`genre_name`,`language`,`album_name`,`album_id`,`singer_str`,`create_time`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            cur.execute(sql, (top_title, top_time,song_name,song_id, song_mid,song_long_time,song_time_public,company_name,company_id, genre_name, language, album_name,album_id, singer_str, create_time))
            sql_client.commit()
        except:
            print(sql)

    for song in song_list:
        # song_dict = {}
        song_top = song['rank'] # 歌曲排名
        song_name = song['title'] #歌曲名称
        song_id = song['songId'] #歌曲ID
        singer_name = song['singerName'] #歌手名
        singer_mid = song['singerMid'] #歌手Mid
        create_time = time.strftime('%Y-%m-%d-%H',time.localtime(time.time()))

        try:
            sql = 'insert into `qq_music_top`(`top_title`,`top_time`,`song_top`,`song_name`,`song_id`,`singer_name`,`singer_mid`,`create_time`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
            cur.execute(sql,(top_title,top_time,song_top,song_name,song_id,singer_name,singer_mid,create_time))
            sql_client.commit()
        except:
            print(sql)

# 请求并获取歌曲详情页json信息
def get_music_detail(song_id,song_mid):
    json_url = 'https://u.y.qq.com/cgi-bin/musicu.fcg?data={%22comm%22:{%22ct%22:24,%22cv%22:0},%22songinfo%22:{%22method%22:%22get_song_detail_yqq%22,%22param%22:{%22song_type%22:0,%22song_mid%22:%22'+song_mid+'%22,%22song_id%22:'+str(song_id)+'},%22module%22:%22music.pf_song_detail_svr%22}}'
    json_msg = requests.get(json_url,headers = headers).text
    json_msg1 = json.loads(json_msg)

    data = json_msg1['songinfo']['data']
    # print(json_msg1)
    try:
        company_name = data['info']['company']['content'][0]['value'] # 唱片公司名字
    except:
        company_name = ''
    try:
        company_id = str(data['info']['company']['content'][0]['id'])
    except:
        company_id = ''
    try:
        genre_name = data['info']['genre']['content'][0]['value'].strip()# 专辑
    except:
        genre_name = ''
    try:
        language = data['info']['lan']['content'][0]['value'].strip()# 语种
    except:
        language = ''
    try:
        album_name = data['track_info']['album']['name'] # 流派
    except:
        album_name = ''
    try:
        album_id = str(data['track_info']['album']['id'])  # 流派
    except:
        album_id = ''
    try:
        singer = data['track_info']['singer'][0]['name'] #歌手
    except:
        singer = ''
    return (company_name,company_id,genre_name,language,album_name,album_id,singer)


# 执行函数
if __name__ == '__main__':
    # 通过排行榜aip入口获取各排行榜接口topID
    url = 'https://y.qq.com/n/yqq/toplist/4.html'
    detail_url = get_top_urllist(url)
    for urls in detail_url:
        top_id = re.findall('html#stat=.*?(\d+)',urls)
        if top_id:
            if int(top_id[0]) in [4,27,108,123,106,107,105,113,114,103]:
                get_top_msg(top_id[0])