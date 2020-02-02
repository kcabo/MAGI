import datetime
import os
import threading

from flask import Flask, request, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy

if os.name == 'nt': # ローカルのWindows環境なら、環境変数をその都度設定
    import env

import analyzer
from constant import FormatEvent, japanese_grades
import scraper
from task_manager import Takenoko, status, notify_line

app = Flask(__name__)
app.config.from_object('config.Develop' if os.name == 'nt' else 'config.Product')
db = SQLAlchemy(app)
manegement_url = os.environ['ADMIN_URL'] # 秘密の管理用のURLを環境変数から取得


class Meet(db.Model):
    __tablename__ = 'meets'
    id = db.Column(db.Integer, primary_key=True)                      # 自動で連番で振られるid
    meetid = db.Column(db.Integer, unique = True, nullable = False)   # 7桁の大会ID 0119721など0で始まる場合は6桁になる
    name = db.Column(db.String, nullable = False)                     # 大会名
    place = db.Column(db.String, nullable = False)                    # 会場
    pool = db.Column(db.Integer, nullable = False)                    # 0 (短水路) or 1(長水路)
    start = db.Column(db.Integer, nullable = False)                   # 大会開始日 20190924 の整数型で表す
    end = db.Column(db.Integer, nullable = False)                     # 大会終了日
    area = db.Column(db.Integer, nullable = False)                    # 地域(整数2桁)
    year = db.Column(db.Integer, nullable = False)                    # 開催年(2桁)
    code = db.Column(db.Integer, nullable = False)                    # 下三桁

    def __str__(self):
        return str((self.id, self.meetid, self.name, self.pool, self.start))

    def __init__(self, meet_id): # 渡されるのは整数
        self.meetid = meet_id    # 十万から900万くらいまで
        self.area = meet_id // 100000
        tmp = meet_id % 100000   # エリアコード省いた値。19721とかになる
        self.year = tmp // 1000
        self.code = tmp % 1000   # 下三桁
        self.start, self.end, self.name, self.place, self.pool = scraper.meet_info(meet_id)


class Record(db.Model): # 個人種目とリレーの記録
    __tablename__ = 'records'
    id = db.Column(db.Integer, primary_key=True)                      # 自動で連番で振られるid
    meetid = db.Column(db.Integer, nullable = False)                  # 7桁の大会ID 0119721など0で始まる場合は6桁になる
    event = db.Column(db.Integer, nullable = False)                   # 性別・スタイル・距離をつなげた整数
    relay = db.Column(db.Integer, nullable = False)                   # 個人種目なら0、リレー一泳なら1,以後2,3,4泳。リレー全体記録なら5。
    rank = db.Column(db.String, nullable = False)                     # 順位や棄権、失格情報など
    name = db.Column(db.String, nullable = False)                     # 選手氏名。リレーのときはカンマでつなげて4人
    team = db.Column(db.String, nullable = False)                     # 所属名
    grade = db.Column(db.Integer, nullable = False)                   # 学年 小学1年生 1 から 一般19まで リレー記録は0 無効な学年は-1
    time = db.Column(db.Integer, nullable = False)                    # タイム。百倍秒数(hecto_seconds)。失格棄権は0。意味不明タイムは-1
    laps = db.Column(db.String, nullable = False)                     # ラップタイム。百倍秒数をカンマでつなげる

    def __init__(self, meet_id, event, relay, rank, name, team, grade, time, laps):
        self.meetid, self.event, self.relay, self.rank, self.name, self.team, self.grade, self.time, self.laps = (
            meet_id, event, relay, rank, name, team, grade, time, laps)


class Stats(db.Model): #種目の平均値、標準偏差
    __tablename__ = 'stats'
    id = db.Column(db.Integer, primary_key=True)                      # 自動で連番で振られるid
    pool = db.Column(db.Integer, nullable = False)                    # 0 (短水路) or 1(長水路)
    event = db.Column(db.Integer, nullable = False)                   # 性別・スタイル・距離をつなげた整数 25mも含める 混合は含めない
    agegroup = db.Column(db.Integer, nullable = False)                # 0全体・1小学・2中学・3高校・4大学・5一般
    mean = db.Column(db.Float)                                        # タイムの平均値 100倍秒数値
    std = db.Column(db.Float)                                         # 標準偏差
    q1 = db.Column(db.Integer)                                        # 第一四分位 百倍秒数。小さい、つまり速い方
    q2 = db.Column(db.Integer)                                        # 第二四分位。中央値 百倍秒数
    q3 = db.Column(db.Integer)                                        # 第三四分位 百倍秒数
    border = db.Column(db.Integer)                                    # 500番目のタイム。百倍秒数
    count = db.Column(db.Integer)                                     # その種目のランキング化したあとの人数 外れ値含む

    def __init__(self, pool, event, agegroup):
        self.pool, self.event, self.agegroup = pool, event, agegroup


def initialize_stats_table():
    from constant import style_and_distance
    for pool in [0, 1]:
        for sex in [1, 2]:
            for sd in style_and_distance:
                for agegroup in [0, 1, 2, 3, 4, 5]:
                    event = sex * 100 + sd
                    row = Stats(pool, event, agegroup)
                    db.session.add(row)
    db.session.commit()


def analyze_all(year):
    # statisticsテーブルの行を一行ずつ見ていき、それぞれアップデート
    notify_line('全696種目の記録分布の分析を開始')
    stats = db.session.query(Stats).all()
    for st in Takenoko(stats, 20):
        records = (db.session.query(Record, Meet)
            .filter(Record.event==st.event, Record.time > 0, Record.meetid == Meet.meetid, Meet.pool == st.pool, Meet.year == year).all())
        st.mean, st.std, st.q1, st.q2, st.q3, st.border, st.count = analyzer.compile_statistics(records, st.agegroup)
        del records
        db.session.commit()
    notify_line('全種目の分析を完了')
    status.free()

def calc_deviation(value, mean, std): # 無効の場合ハイフン
    if value and mean and std:
        answer = (value - mean) / std * -10 + 50 # 数値が少ないほうが高くしたいので－10かけ
        return round(answer, 1)
    else:
        return '-'


def add_records(target_meets_ids): # 大会IDのリストから１大会ごとにRecordの行を生成しDBに追加
    notify_line(f">>> {len(target_meets_ids)}の大会の全記録の抽出開始")
    count_records = 0
    for meet_id in Takenoko(target_meets_ids, 20):
        records = []
        for event in scraper.all_events(meet_id):
            records.extend([Record(*args) for args in event.crawl_table()])

        # ひとつの大会が終わるごとにもともとあったレコードを削除して、新たにゲットしたのを追加
        db.session.query(Record).filter_by(meetid=meet_id).delete()
        db.session.add_all(records)
        count_records += len(records)
        del records
        db.session.commit()

    notify_line(f'>>> 全{count_records}件の記録の保存完了 現在：{format(total_count(), ",")}件')
    status.free()

def add_meets(year):
    print(f">>> 20{year}年開催の大会IDの収集を開始")
    meet_ids = [] # 整数型を入れる
    # for area_int in Takenoko(list(range(1, 54)) + [70,80]): # 1から53までと全国70国際80がarea番号になる
    for area_int in Takenoko(range(14,16)): # ローカル用
        meet_ids.extend(scraper.find_meet(year, format(area_int, '02'))) # ゼロ埋め
    print(f'>>> 20{year}年に開催される全{len(meet_ids)}の大会情報を取得中')
    meets = [Meet(id) for id in Takenoko(meet_ids, 20)]
    db.session.query(Meet).filter_by(year = year).delete() # 同じ年度を二重に登録しないように削除する
    db.session.add_all(meets)
    db.session.commit()
    print(f'>>> 全{len(meets)}の大会情報の保存が完了')
    status.free()

def total_count():
    count = db.session.query(Record).count()
    return count


####### 以下ルーター #######
@app.route('/')
def index():
    return render_template('index.html', count_records=total_count())

@app.route('/credits')
def credits():
    return render_template('credits.html')

@app.route('/develop')
def develop():
    return render_template('develop.html')

@app.route('/msg', methods = ['POST'])
def receive_message():
    msg = request.form.getlist("msg")[0]
    notify_line(msg)
    return render_template('index.html', count_records=total_count(), msg=msg)

@app.route('/up')
def wake_up(): # 監視サービスで監視する用のURL
    return 'ok'


@app.route('/dashboard')
def dashboard():
    # name と grade がついていることを保証している
    name = request.args.get('name')
    grade = request.args.get('grade', type=int)

    # 取得した選手の名前・学年でフィルタリングしてrecordsテーブルから取得
    # 同時にそれぞれのrecordのmeetidからMeetを内部結合 年度はとりま19年に指定
    records = (db.session.query(Record, Meet)
            .filter(Record.name == name, Record.grade == grade, Record.meetid == Meet.meetid, Meet.year == 19)
            .all())

    # 見出しの選手情報：     性別　名前　学年　所属一覧
    teams = {r.Record.team for r in records} # set型なので重複削除される
    sex = records[0].Record.event // 100 # ひとつめのレコードのeventの百の位
    swimmer = analyzer.Swimmer(records)
    del records # メモリ削減。効果ないかも
    swimmer.sex = 'men' if sex == 1 else 'women'
    swimmer.name = name
    swimmer.grade_jp = japanese_grades[grade]
    swimmer.grade = grade
    swimmer.teams = teams

    # 偏差値の導出
    event_code = swimmer.events[0].code
    if event_code:
        # 0全体・1小学・2中学・3高校・4大学・5一般
        agegroup_list = [0,1,1,1,1,1,1,2,2,2,3,3,3,4,4,4,4,4,4,5]
        agegroup = agegroup_list[grade] # gradeからagegroupへの変換 gradeは1以上なので最初の0が選ばれることはない
        stats = db.session.query(Stats).filter_by(event=event_code, agegroup=agegroup).order_by(Stats.pool).all() # 1番目が短水路、2番目が長水路になる
        dev_short = calc_deviation(swimmer.e1bests[0], stats[0].mean, stats[0].std)
        dev_long = calc_deviation(swimmer.e1bests[1], stats[1].mean, stats[1].std)
        deviation = dev_long if dev_long != '-' else dev_short
    else:
        deviation = '-'

    if deviation == '-':
        mask_height = 100
    elif deviation >= 75:
        mask_height = 0
    else:
        mask_height = 75 - deviation
    swimmer.deviation = deviation
    swimmer.mask_height = mask_height

    # バッジの格納
    icons = []
    if [True for t in teams if t in ['JPN', 'JAPAN', '日本']]:
        icons.append('fa-users')
    if [True for t in teams if t in ['慶應義塾大', 'KEIO', '慶應義塾大学', '慶応', '慶応女子', '慶應志木', '慶應', '慶應湘南藤沢', '慶應湘南', '慶應普通部', '銀泳会']]:
        icons.append('fa-pen-nib')
    if name == '神崎伶央':
        icons.append('fa-pastafarianism')
    if deviation >= 65:
        icons.append('fa-star')
    if deviation >= 70:
        icons.append('fa-chess-king')
    if swimmer.total_count >= 50:
        icons.append('fa-fist-raised')
    # <i class="fas fa-dragon"></i>
    swimmer.icons = icons

    return render_template('dashboard.html', s = swimmer)


@app.route('/ranking',  methods = ['POST', 'GET'])
def ranking():
    event = request.args.get('event', 112, type=int)
    year = request.args.get('year', 19, type=int)
    pool = request.args.get('pool', 1, type=int)
    all = request.args.get('all', 0, type=int)
    grades = request.form.getlist("grade", type=int) # POST時のフォームの内容が格納されるGET時は空リスト

    if grades: # 学年絞り込み指定ある時
        records = (db.session.query(Record, Meet)
                .filter(Record.event==event, Record.grade.in_(grades), Record.time > 0, Record.meetid == Meet.meetid, Meet.pool == pool, Meet.year == year)
                .all())
    elif all == 0: # もっとみる、を押す前
        target_event = db.session.query(Stats).filter_by(pool=pool, event=event, agegroup=0).one()
        time_limit = target_event.border
        records = (db.session.query(Record, Meet)
                .filter(Record.event==event, Record.time > 0, Record.time <= time_limit, Record.meetid == Meet.meetid, Meet.pool == pool, Meet.year == year)
                .all())
    else:
        records = (db.session.query(Record, Meet)
                .filter(Record.event==event, Record.time > 0, Record.meetid == Meet.meetid, Meet.pool == pool, Meet.year == year)
                .all())

    df = analyzer.format_ranking(analyzer.output_ranking(records))
    # {% for id, new, name, time, grade, grade_jp, team in ranking %}
    ranking = zip(df['id'], df['new'], df['name'], df['time'], df['grade'], df['grade_jp'], df['team'])
    my_event = FormatEvent(event)
    return render_template(
            'ranking.html',
            ranking = ranking,
            jpn_event = my_event.jpn_event(),
            year = year,
            sex = event // 100,
            pool = pool,
            style = event % 100,
            grades = grades,
            all = all)


@app.route('/apiResult', methods=['POST'])
def result_detail():
    body = request.get_json()
    id = body['id']
    target = db.session.query(Record, Meet).filter(Record.id==id, Record.meetid == Meet.meetid).first()
    res = analyzer.detail_dictionary(target)
    agegroup_list = [0,1,1,1,1,1,1,2,2,2,3,3,3,4,4,4,4,4,4,5] # 0全体・1小学・2中学・3高校・4大学・5一般
    agegroup = agegroup_list[target.Record.grade] # gradeからagegroupへの変換 gradeは1以上なので最初の0が選ばれることはない
    stats_agegroup = db.session.query(Stats).filter_by(event=target.Record.event, agegroup=agegroup, pool=target.Meet.pool).first()
    stats_whole = db.session.query(Stats).filter_by(event=target.Record.event, agegroup=0, pool=target.Meet.pool).first()
    res['dev1'] = calc_deviation(target.Record.time, stats_whole.mean, stats_whole.std)
    res['dev2'] = calc_deviation(target.Record.time, stats_agegroup.mean, stats_agegroup.std)
    return jsonify(res)


@app.route('/apiRank', methods=['POST'])
def time_and_rank():
    body = request.get_json()
    index = body['index']
    time_val = body['time_val']
    event = body['event_code']
    pool = body['pool']
    grade = body['grade']
    year = 19

    if event:
        target_stats = db.session.query(Stats).filter_by(event=event, agegroup=0, pool=pool).one()
        whole_count = target_stats.count
        same_count = (db.session.query(Record, Meet)
                .filter(Record.event == event, Record.time > 0, Record.grade == grade, Record.meetid == Meet.meetid, Meet.pool == pool, Meet.year == year)
                .distinct(Record.name)
                .count())
    else:
        whole_count = '-'
        same_count = '-'

    if time_val:
        # 自分より速いスイマーの数を数える
        whole_count_faster_swimmer = (db.session.query(Record, Meet)
                .filter(Record.event == event, Record.time > 0, Record.time < time_val, Record.meetid == Meet.meetid, Meet.pool == pool, Meet.year == year)
                .distinct(Record.name, Record.grade)
                .count())
        whole_ranking = whole_count_faster_swimmer + 1

        same_count_faster_swimmer = (db.session.query(Record, Meet)
                .filter(Record.event == event, Record.time > 0, Record.time < time_val, Record.grade == grade, Record.meetid == Meet.meetid, Meet.pool == pool, Meet.year == year)
                .distinct(Record.name)
                .count())
        same_ranking = same_count_faster_swimmer + 1

    else:
        whole_ranking = '-'
        same_ranking = '-'

    time = analyzer.val_2_fmt(time_val)

    rtn = {
        'index': index,
        'time': time if time else '-',
        'same_ranking': same_ranking,
        'same_count': same_count,
        'whole_ranking': whole_ranking,
        'whole_count': whole_count
    }
    return jsonify(rtn)


@app.route('/search')
def search():
    query = request.args.get('query', '').replace(' ','').replace('_','').replace('%','')
    if query:
        records = db.session.query(Record).filter(Record.name.like(f"%{query}%"), Record.relay == 0).all()
        team_mates = db.session.query(Record).filter(Record.team.like(f"%{query}%"), Record.relay == 0).all()
        # team_matesは検索したチームから出た記録しか抽出されないので、各選手の他のチームから出た記録も検索
        names = {m.name for m in team_mates} # 検索したチームに所属する選手の他の所属を検索
        records_with_another_team = db.session.query(Record).filter(Record.name.in_(names), Record.relay == 0).all()
        records.extend(records_with_another_team)
        del records_with_another_team
    else:
        records = []

    men, women = analyzer.raise_candidates(records)
    show_sorry = False if men or women else True
    return render_template(
            'search.html',
            query = query,
            men = men,
            women = women,
            show_sorry = show_sorry)


def check_is_busy():
    s = status.get_status()
    is_busy = True if s == 'busy' else False
    return is_busy

def all_threads():
    thread_list = [t.name for t in threading.enumerate()] # 起動中のスレッド一覧を取得
    threads = ' / '.join(thread_list)
    return threads

@app.route(manegement_url)
def admin_no_offer():
    return render_template(
        'admin.html',
        is_busy = check_is_busy(),
        threads = all_threads(),
        admin_url = manegement_url
    )

@app.route(manegement_url + '/<command>')
def admin_without_threads(command):
    msg = ''
    obj = ''
    if command == 'create':
        db.create_all()
        obj = 'create_all'

    elif command == 'drop':
        db.drop_all()
        obj = 'drop_all'

    elif command == 'foreign':
        from constant import foreign_teams
        count = db.session.query(Record).filter(Record.team.in_(foreign_teams)).count()
        # リストでフィルターをかけているが、deleteの引数synchronize_sessionのデフォルト値'evaluate'ではこれをサポートしていない(らしい)からFalseを指定する
        db.session.query(Record).filter(Record.team.in_(foreign_teams)).delete(synchronize_session = False)
        db.session.commit()
        msg = f'total: {count}'
        obj = 'erase_foreign'

    elif command == 'init_stats':
        db.session.query(Stats).delete()
        initialize_stats_table()
        obj = 'init_stats'

    return render_template(
        'admin.html',
        show_pane = True,
        is_done = True,
        obj = obj,
        is_busy = check_is_busy(),
        threads = all_threads(),
        admin_url = manegement_url
    )

@app.route(manegement_url + '/thread/<command>')
def admin_with_threads(command): # 並列処理実行
    is_busy = check_is_busy()
    show_pane = False
    show_rejected = True
    invalid_url = True
    obj = ''
    msg = ''

    if is_busy:
        invalid_url = False

    elif command in ['meets', 'records', 'stats']:
        range = request.args.get('range', default=None, type=int)
        date_min = request.args.get('from', default="20190401")
        date_max = request.args.get('to', default="20190404")
        year = request.args.get('year', default=19, type=int)

        show_pane = True
        show_rejected = False
        if command == 'meets':
            th = threading.Thread(target=add_meets, name='scraper_m', args=(year,))
            obj = 'add_meets'
            msg = f'year: {year}'

        elif command == 'records':
            if range:
                today = datetime.date.today()
                week_ago = today - datetime.timedelta(days=range)
                date_min = week_ago.strftime('%Y%m%d')
                date_max = today.strftime('%Y%m%d')
            target_meets = db.session.query(Meet).filter(Meet.start >= int(date_min), Meet.start <= int(date_max)).order_by(Meet.start).all()
            # 古いのからやるように！！！！！
            target_meets_ids = [m.meetid for m in target_meets]
            th = threading.Thread(target=add_records, name='scraper_r', args=(target_meets_ids,))
            obj = 'add_records'
            msg = f'from: {date_min}  to: {date_max}'

        elif command == 'stats':
            th = threading.Thread(target=analyze_all, name='stats', args=(year,))
            obj = 'analyze_all'
            msg = f'year: {year}'

        status.busy()
        db.session.commit()
        th.start()
    return render_template(
        'admin.html',
        show_pane = show_pane,
        is_done = False,
        obj = obj,
        msg = msg,
        show_rejected = show_rejected,
        invalid_url = invalid_url,
        is_busy = True,
        threads = all_threads(),
        admin_url = manegement_url
    )


if __name__ == "__main__": #gunicornで動かす場合は実行されない
    print('組み込みサーバーで起動します')
    app.run()
