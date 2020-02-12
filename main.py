import datetime
import sys

import sqlalchemy as db
from sqlalchemy.orm import aliased
from sqlalchemy import func, desc
import pandas as pd

import scraper
from task_manager import Takenoko, notify_line
from config import session, Base, engine

CURRENT_YEAR = 19

class Meet(Base):
    __tablename__ = 'meets'
    meet_id = db.Column(db.Integer, primary_key=True, autoincrement=False) # 7桁の大会ID 0119721など0で始まる場合は6桁になる
    meet_name = db.Column(db.String, nullable = False)
    place = db.Column(db.String, nullable = False)   # 会場
    pool = db.Column(db.Integer, nullable = False)   # 0 (短水路) or 1(長水路)
    start = db.Column(db.Integer, nullable = False)  # 大会開始日 20190924 の整数型で表す
    end = db.Column(db.Integer, nullable = False)    # 大会終了日
    area = db.Column(db.Integer, nullable = False)   # 地域(整数2桁)
    year = db.Column(db.Integer, nullable = False)   # 開催年(2桁)

class Record(Base): # 個人種目とリレーの記録
    __tablename__ = 'records'
    record_id = db.Column(db.Integer, primary_key=True)
    meet_id = db.Column(db.Integer, nullable = False)
    event = db.Column(db.Integer, nullable = False)   # 性別・スタイル・距離をつなげた整数
    relay = db.Column(db.Integer, nullable = False)   # 個人種目なら0、リレー一泳なら1,以後2,3,4泳。リレー全体記録なら5。
    rank = db.Column(db.String, nullable = False)     # 順位や棄権、失格情報など
    swimmer_id = db.Column(db.Integer, nullable = False)
    team_id = db.Column(db.Integer, nullable = False)
    time = db.Column(db.Integer, nullable = False)    # タイム。百倍秒数(hecto_seconds)。失格棄権は0。意味不明タイムは-1
    laps = db.Column(db.String, nullable = False)     # ラップタイム。百倍秒数をカンマでつなげる

    def __init__(self, meet_id, event, relay, rank, name, team, grade, time, laps):
        self.meet_id, self.event, self.relay, self.rank, self.name, self.team, self.grade, self.time, self.laps = (
            meet_id, event, relay, rank, name, team, grade, time, laps)

    def set_team(self):
        team = self.team
        exists = session.query(Team.team_id).filter_by(team_name=team).first()
        if exists:
            self.team_id = exists.team_id
        else:
            new_team = Team(team_name=team)
            session.add(new_team)
            session.flush()
            self.team_id = new_team.team_id

    def set_swimmer(self):
        name = self.name
        grade = self.grade
        year = (self.meet_id % 100000) // 1000

        match = session.query(Swimmer.swimmer_id, Swimmer.sex).filter(
            Swimmer.name == name,
            getattr(Swimmer, f'grade_{year}') == grade # 同じ年に同じ学年で同じ名前の人がいたら特定
            ).first()

        if match: # ここで見つかる場合が最も多いはず
            self.swimmer_id = match.swimmer_id
            # もともとの性別が混合3であり、かつこの種目が混合じゃないなら性別情報を修正
            if match.sex == 3 and (sex := self.event // 100) != 3:
                print('性別情報を修正',sex, self.swimmer_id, match)
                match.sex = sex

        # しかし運悪く見つからず、その年で初めて出場する場合
        elif same_names := session.query(Swimmer).filter(
                Swimmer.name == name,
                # filterではis演算子に対応してない その年の学年はまだ未設定の人（設定してあって一致するなら上のmatchになる）
                getattr(Swimmer, f'grade_{year}') == None
            ).all():

            found = False
            for suggest in same_names: # 特定開始 同姓同名の候補者から学年をもとに同一選手を探す
                for gap in [-1, 1, -2, 2]: # 一年前、1年後、2年前、2年後を順に探索
                    another_grade = getattr(suggest, f'grade_{year + gap}', None) # year+gapが16とかのときattrに存在しない
                    if another_grade: # 別の年の学年を持っていたら候補をあげる
                        estimated_grades = estimate_other_grades(grade, gap)
                        if another_grade in estimated_grades:
                            # 候補の持っていた学年が予測に一致したため、特定したとする
                            self.swimmer_id = suggest.swimmer_id
                            setattr(suggest, f'grade_{year}', grade) # 今年の学年をセット
                            found = True
                            break

            if found == False: # 同じ選手特定できずなら追加 （同姓同名の別の選手がいたってこと）
                self.add_swimmer(year)

        else: # テーブルに無い選手の場合、選手追加
            self.add_swimmer(year)

    def add_swimmer(self, year):
        new_swimmer = Swimmer(name=self.name, sex=self.event // 100)
        setattr(new_swimmer, f'grade_{year}', self.grade)
        session.add(new_swimmer)
        session.flush()
        self.swimmer_id = new_swimmer.swimmer_id

def estimate_other_grades(original_grade, gap):
    # 中学1年生はgap年後には何年生か？ gapがマイナスなら過去の学年を返す
    # 留年の場合を考えない
    if original_grade == 19:
        if gap == -1:
            return [16, 18, 19] # 一般の一年前
        elif gap == -2:
            return [15, 17, 19] # 一般の二年前
        else: # 一般は未来も常に一般
            return [19]
    elif gap == 2 and original_grade == 18:
        return [19]
    elif gap == 1 and original_grade == 16:
        return [17, 19] # 大学4年生の一年後は大学5年生か一般
    elif gap == 2 and original_grade == 16:
        return [18, 19] # 大学4年生の二年後は大学6年生か一般
    elif gap == 2 and original_grade == 15:
        return [17, 19] # 大学三年生の二年後は五年生か一般
    elif gap == -2 and original_grade == 2:
        return [] # 小二に二年前はない
    elif gap < 0 and original_grade == 1:
        return [] # 小1に過去はない
    else:
        return [original_grade + gap]


class Swimmer(Base):
    __tablename__ = 'swimmers'
    swimmer_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable = False)
    sex = db.Column(db.Integer) # 1男子 2女子 3混合 0リレー
    awards = db.Column(db.Integer, default = 1)
    visits = db.Column(db.Integer, default = 0)
    read = db.Column(db.String)
    grade_17 = db.Column(db.Integer)
    grade_18 = db.Column(db.Integer)
    grade_19 = db.Column(db.Integer)
    grade_20 = db.Column(db.Integer)
    grade_21 = db.Column(db.Integer)


class Team(Base):
    __tablename__ = 'teams'
    team_id = db.Column(db.Integer, primary_key=True)
    team_name = db.Column(db.String, nullable = False)
    area = db.Column(db.Integer)
    alias = db.Column(db.Integer) # 別名のチームID
    team_read = db.Column(db.String)


class Stats(Base): #種目の平均値、標準偏差
    __tablename__ = 'stats'
    stats_id = db.Column(db.Integer, primary_key=True)     # 自動で連番で振られるid
    pool = db.Column(db.Integer, nullable = False)         # 0 (短水路) or 1(長水路)
    event = db.Column(db.Integer, nullable = False)        # 性別・スタイル・距離をつなげた整数 25mも含める 混合は含めない
    grade = db.Column(db.Integer, nullable = False)        # 0 が全体 その後19まで各学年
    mean = db.Column(db.Float)                             # タイムの平均値 100倍秒数値
    std = db.Column(db.Float)                              # 標準偏差
    q1 = db.Column(db.Integer)                             # 第一四分位 百倍秒数。小さい、つまり速い方
    q2 = db.Column(db.Integer)                             # 第二四分位。中央値 百倍秒数
    q3 = db.Column(db.Integer)                             # 第三四分位 百倍秒数
    border = db.Column(db.Integer)                         # 500番目のタイム。百倍秒数
    count_agg = db.Column(db.Integer)                      # 現在の年度の全記録数
    count_ranking = db.Column(db.Integer)                  # 現在の年度のランキング人数


def set_conditions(pool, event, year=None, grade=None):
    # query内で使用する条件文のリスト
    conditions = [
        Record.meet_id == Meet.meet_id,
        Record.swimmer_id == Swimmer.swimmer_id,
        Meet.pool == pool,
        Record.event == event,
        Record.time > 0
    ]
    if year:
        conditions.append(Meet.year == year)
        if grade:  # grade0のときは全学年検索
            conditions.append(getattr(Swimmer, f'grade_{year}') == grade)
    return conditions


def opt_out_foreigners():
    from constant import foreign_teams
    f_team_query = session.query(Team.team_id).filter(Team.team_name.in_(foreign_teams)).all()
    f_team_ids = [t.team_id for t in f_team_query]
    # リストでフィルターをかけているが、deleteの引数synchronize_sessionのデフォルト値'evaluate'ではこれをサポートしていない(らしい)からFalseを指定する
    count = session.query(Record).filter(Record.team_id.in_(f_team_ids)).delete(synchronize_session = False)
    session.commit()
    notify_line(f'{len(f_team_ids)}件の外国籍チームを検出。{count}件の記録を削除')


def analyze_all(year):
    # statisticsテーブルの行を一行ずつ見ていき、それぞれアップデート
    notify_line('全記録分析を開始')
    stats_table = session.query(Stats).all()

    for stat in Takenoko(stats_table, 20):
        conditions = set_conditions(stat.pool, stat.event, year, stat.grade)
        stmt = session.query(
                Record.time
            ).distinct(
                Record.swimmer_id
            ).filter(
                *conditions
            ).order_by(
                Record.swimmer_id,
                Record.time
            ).subquery()

        subq = aliased(Record, stmt)
        times = session.query(stmt).order_by(subq.time).all()
        count_ranking = len(times)
        stat.count_ranking = count_ranking
        stat.count_agg = session.query(func.count(Record.record_id)).filter(*conditions).scalar()

        if count_ranking >= 2:
            stat.border = times[499].time if count_ranking >= 500 else None
            vals = pd.Series([t.time for t in times])

            # 外れ値除くための範囲を決める
            q1 = vals.quantile(.25)
            q3 = vals.quantile(.75)
            iqr = q3-q1
            lower_limit = q1 - iqr * 1.5
            upper_limit = q3 + iqr * 1.5

            # 外れ値除外したやつの記述統計量を取得
            desc = vals[(vals > lower_limit) & (vals < upper_limit)].describe()
            stat.mean = round(desc['mean'], 2) # 小数点第2位までで四捨五入
            stat.std = round(desc['std'], 2)
            stat.q1 = int(desc['25%'])
            stat.q2 = int(desc['50%'])
            stat.q3 = int(desc['75%'])

        session.commit()
    notify_line('全記録の分析を完了')




def add_records(target_meets_ids): # 大会IDのリストから１大会ごとにRecordの行を生成しDBに追加
    notify_line(f"{len(target_meets_ids)}の大会の全記録の抽出開始")
    record_length = 0
    erased = 0

    for meet_id in Takenoko(target_meets_ids, 20):
        events_array = scraper.all_events(meet_id)
        for event in events_array:
            records = [Record(*args) for args in event.crawl_table()]
            total_time = sum([r.time for r in records])
            current_total_time = session.query(func.sum(Record.time)).filter_by(meet_id=event.meet_id, event=event.event_id).scalar()
            if total_time != current_total_time: # タイムの合計が一致してたらわざわざ削除してセットし直すこともない
                erased += session.query(Record).filter_by(meet_id=event.meet_id, event=event.event_id).delete()
                for rc in records:
                    rc.set_team()
                    rc.set_swimmer()
                session.add_all(records)
                record_length += len(records)
                session.commit()

    notify_line(f'{erased}件を削除 {record_length}件を新規に保存 現在：{format(count_records(), ",")}件')


def add_meets(year, force=False):
    notify_line(f"大会情報の収集を開始。対象:20{year}年")
    meet_ids = []
    # for area_int in Takenoko(range(14,15)): # ローカル用
    for area_int in Takenoko(list(range(1, 54)) + [70,80]): # 本番用 1から53までと全国70国際80がarea番号になる
        meet_ids.extend(scraper.find_meet(year, area_int))

    saved_meets = session.query(func.sum(Meet.meet_id)).filter_by(year=year).scalar()
    if force or sum(meet_ids) != saved_meets: # 大会IDの合計値が一致しないか、強制実行の場合
        notify_line(f'全{len(meet_ids)}の大会を検出')

        meets = []
        for id in Takenoko(meet_ids, 20):
            area = id // 100000
            year = (id % 100000) // 1000
            start, end, name, place, pool = scraper.meet_info(id)
            meets.append(Meet(
                meet_id=id,
                meet_name=name,
                place=place,
                pool=pool,
                start=start,
                end=end,
                area=area,
                year=year
            ))

        erased = session.query(Meet).filter_by(year=year).delete() # 同じ年度を二重に登録しないように削除する
        session.add_all(meets)
        session.commit()
        notify_line(f'{erased}件を削除 全{len(meets)}の大会情報を保存')

    else:
        notify_line(f'大会情報に更新はありませんでした')


def count_records():
    count = session.query(func.count(Record.record_id)).scalar()
    return count

def initialize_stats_table():
    session.query(Stats).delete()
    session.commit()
    for pool in [0, 1]:
        for sex in [1, 2]:
            for style_and_distance in [11,12,13,14,15,16,17,21,22,23,24,31,32,33,34,41,42,43,44,53,54,55,63,64,65,66,73,74,75]:
                for grade in range(20):
                    event = sex * 100 + style_and_distance
                    session.add(Stats(pool=pool, event=event, grade=grade))
    session.commit()

def add_records_wrapper(date_min, date_max):
    target_meets = session.query(
            Meet.meet_id
        ).filter(
            Meet.start >= date_min,
            Meet.start <= date_max
        ).order_by(
            Meet.start
        ).all()
    target_meets_ids = [m.meet_id for m in target_meets]
    add_records(target_meets_ids)

def routine(year=None, date_min=None, date_max=None):
    # def add_meets(year, force=False):
    # def add_records_wrapper(date_min, date_max):
    # def opt_out_foreigners():
    # def analyze_all(year):
    today = datetime.date.today()

    if year is None:
        year = CURRENT_YEAR

    if date_max is None:
        date_max = int(today.strftime('%Y%m%d'))

    if date_min is None:
        day_range = 7
        date_min_obj = today - datetime.timedelta(days=day_range)
        date_min = int(date_min_obj.strftime('%Y%m%d'))

    # 水曜日のみ強制更新
    force = True if today.weekday() == 2 else False

    add_meets(year, force)
    add_records_wrapper(date_min, date_max)
    opt_out_foreigners()
    analyze_all(year)


if __name__ == '__main__':
# Base.metadata.drop_all(bind=engine)
# Base.metadata.create_all(bind=engine)
# initialize_stats_table()
    args = sys.argv
    if len(args) == 1:
        # routine()
        # std = datetime.datetime(2020, 2, 10, 1, 44)
        # now = datetime.datetime.now()
        # sub = (now - std).seconds // 3600 + (now - std).days * 24
        # print(now, sub)
        #
        # q = session.query(Meet).filter(Meet.year==17).order_by(desc(Meet.start), desc(Meet.meet_id)).all()
        # min = sub * 22
        # max = (sub+1) * 22
        # target_meets = q[min : max]
        # print(f'{target_meets[0].start}の{target_meets[0].meet_id}から{target_meets[-1].start}の{target_meets[-1].meet_id}まで')
        # target_meets_ids = [m.meet_id for m in target_meets]
        # add_records(target_meets_ids)

        target_meets_ids = [4617603,4517705,1117904,217729,2117204,2817601,2117625,2817205,2817616,1017606,2217740,1517759,2517201,2117651,617606,2217751,2917624,4417703,2417607,2217625,2617603,3317601,317601,5217001,4117704,3617604,2817603,1917662,1117604,2817619,3017608,2317752,2817615,1317501,5317405,2517704,2217727,3217721,2717635,117731,2417604,517616,4317741,5017409,4717715,1517726,1117630,2117694,5117401,2017651,117722,1417701,2917703,1017601,1617771,2817501,417201,3017709,1717251,2817718,3717703,4217604,2417701,3117703,2517709,617612,1317702,3817713]

        add_records(target_meets_ids)

    else:
        target = args[1]
        if target == 'add_meets':
            add_meets(int(args[2]))
        elif target == 'routine':
            routine(date_min=int(args[2]), date_max=int(args[3]))
        elif target == 'amari':
            routine(date_min=20200112, date_max=20200119)
        else:
            print(args)
