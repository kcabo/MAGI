import re
import requests
from time import sleep

from bs4 import BeautifulSoup, element

from constant import japanese_grades
from task_manager import notify_line

space_erase_table = str.maketrans("","","\n\r 　 *") # 第三引数に指定した文字が削除される。左から、LF,CR,半角スペース,全角スペース,nbsp
space_and_nums = str.maketrans("","","\n\r 　 *1234.")

def del_space(str):
    return str.translate(space_erase_table) if str is not None else ""

def del_numspace(str):
    return str.translate(space_and_nums)


time_format_ptn = re.compile(r'([0-9]{0,2}):?([0-9]{2}).([0-9]{2})')
meet_link_ptn = re.compile(r"code=[0-9]{7}$")           # <a href="../../swims/ViewResult?h=V1000&amp;code=0119605"
meet_caption_ptn = re.compile(r"(.+)　（(.+)） (.水路)") # 茨城:第42回県高等学校春季　（取手ｸﾞﾘｰﾝｽﾎﾟｰﾂｾﾝﾀｰ） 長水路
event_link_ptn = re.compile(r"&code=(\d{7})&sex=(\d)&event=(\d)&distance=(\d)") # "/swims/ViewResult?h=V1100&code=0919601&sex=1&event=5&distance=4"


def raw_timestr_to_timeval(time_str):
    if time_str in ["", "--:--.--", "-", "ｰ"]: # リレーで第一泳者以外の失格の場合--:--.--になる。最後のはハイフンではなく半角カタカナ長音
        return 0 # 記録なし
    else:
        ob = re.match(time_format_ptn, time_str)
        if ob is None: # 普通じゃ想定していない
            # 大会0119722の平沼さんの女子２フリはこれが適用されている
            notify_line(f'<!!>無効なタイム文字列{time_str}を検出しました。とりま-1を返します')
            return -1
        else:
            min = ob.group(1) if ob.group(1) != "" else 0 # 32.34とか分がないときは分は0
            time_val = int(min)*6000 + int(ob.group(2))*100 + int(ob.group(3))
            return time_val

# DOM探索木をURLから生成
def make_soup(url):
    sleep(0.8) # 負荷軽減用
    req = requests.get(url)
    req.encoding = "cp932"
    return BeautifulSoup(req.text, "lxml")


def meet_info(meet_id):
    str_id = str(meet_id) # 整数で渡されるので6桁の場合もあるからゼロ埋め
    url = f'http://www.swim-record.com/swims/ViewResult/?h=V1000&code={str_id.zfill(7)}'
    soup = make_soup(url)
    caption = soup.find("div", class_ = "headder").find_all("td", class_ = "p14b")

    # 2019/04/27 - 2019/04/27  ←caption[0]
    date = caption[0].string

    def datestr_2_int(date_str): # 2019/04/27を20190427にする
        str = date_str.replace('/', '')
        return int(str)
    start = datestr_2_int(date[:10])
    end = datestr_2_int(date[-10:])

    # 茨城:第42回県高等学校春季　（取手ｸﾞﾘｰﾝｽﾎﾟｰﾂｾﾝﾀｰ） 長水路  ←caption[1]
    meet_title = re.match(meet_caption_ptn, caption[1].string)
    name = meet_title.group(1)
    place = meet_title.group(2)
    pool = 0 if meet_title.group(3)=='短水路' else 1

    return start, end, name, place, pool


# 特定の年度・地域で開催された大会IDのリストを作成するサブルーチン
def find_meet(year, area):
    url = f'http://www.swim-record.com/taikai/{year}/{format(area, "02")}.html' # ゼロ埋め
    soup = make_soup(url)
    #div内での一番最初のtableが競泳大会表。特定パターンのリンクを探す
    meet_id_aTags = soup.find("div", class_ = "result_main").find("table", recursive = False).find_all("a", href = meet_link_ptn)
    id_list = [int(a["href"][-7:]) for a in meet_id_aTags] # 大会コード七桁を検出し整数に変換
    return id_list


class Event:
    def __init__(self, link):   # 一種目の情報とURL 1種目の結果一覧画面に紐付けられている
        matchOb = re.search(event_link_ptn, link) # link = "/swims/ViewResult?h=V1100&code=0919601&sex=1&event=5&distance=4"
        self.url = "http://www.swim-record.com" + link
        self.meet_id = int(matchOb.group(1))
        sex = int(matchOb.group(2))
        style = int(matchOb.group(3))
        distance = int(matchOb.group(4))
        self.event_id = sex * 100 + style * 10 + distance
        self.is_indivisual = style <= 5 # 個人種目(自由形・背泳ぎ・平泳ぎ・バタフライ・個人メドレー)ならTrue

    def crawl(self): # 自身のアトリビュートに記録の一覧をセット
        soup = make_soup(self.url)
        self.rows = soup.find_all("tr", align = "center", bgcolor = False)       # 中央寄せでbgcolorの引数を持たない= レコード行
        self.lap_tables = soup.find_all("tr", align = "right", id = True, style = True) # それぞれのtr内にLAPSテーブルが格納されている

    def parse_table(self): # 記録一覧のページの表を全部とってくる
        set_of_args = []
        for row, lap_table in zip(self.rows, self.lap_tables):
            data = row.find_all("td") # 一行の中に複数のtd(順位、氏名…)(リレーと個人で異なる)が格納されている
            laps_raw = lap_table.find_all("td", width = True) # タイムの書かれたtdのみがwidthの引数を持つ
            laps = [del_space(l.string) for l in laps_raw] # タグを取り除いて空白も削除
            laps_val = ",".join([str(raw_timestr_to_timeval(l)) for l in laps])

            if self.is_indivisual:
                raw_grade = del_space(data[3].string)
                try:
                    grade = japanese_grades.index(raw_grade)
                except ValueError:
                    notify_line(f'無効な学年を検出。{self.url} で{raw_grade}(L={len(raw_grade)})を検出。とりま-1を返しました。')
                    grade = -1
                time_raw = data[4].a
                name = del_space(data[1].string)
            else:
                grade = 0 # リレーに学年は存在しない
                time_raw = data[3].a
                # data[1].contentsはbrタグを含む配列 タグ以外をswimmersに格納
                names = [del_numspace(name) for name in data[1].contents if isinstance(name, element.NavigableString)]
                count = len(names)
                assert count == 1 or count == 4
                if count == 4:
                    name = ','.join(names)
                else: # リレーを棄権したため氏名の表記が無いとき、改行文字だけが検出され、要素数1となる
                    name = ''

            rank = del_space(data[0].text) # data[0].stringだとタグを含んだときにNoneが返されてしまう
            team = del_space(data[2].string)
            time = '' if time_raw is None else del_space(time_raw.string)
            time_val = raw_timestr_to_timeval(time)

            relay = 0 if self.is_indivisual else 5
            # レコードインスタンスを作るのに必要な引数をまとめたタプル
            arguments_for_single_record = (
                self.meet_id, self.event_id, relay, rank, name, team, grade, time_val, laps_val)
            set_of_args.append(arguments_for_single_record)
        return set_of_args

def result_links_in_meet_page(meet_id):
    str_id = str(meet_id)
    url = f'http://www.swim-record.com/swims/ViewResult/?h=V1000&code={str_id.zfill(7)}'
    soup = make_soup(url)
    aTags = soup.find_all("a", class_=True)             # 100m自由形などへのリンクをすべてリストに格納
    links = [a['href'] for a in aTags]
    return links

def all_events(meet_id):
    # 大会IDを指定し、そこから全開催種目を抜き出す。そしてそれらをEventインスタンスにする
    # つまりひとつの大会内での全種目の情報を返す
    event_links = result_links_in_meet_page(meet_id)
    return [Event(link) for link in event_links]
