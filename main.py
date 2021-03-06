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
                update = session.query(Swimmer).get(self.swimmer_id)
                update.sex = sex
                session.commit()

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
        # リレーの全体記録ならis_indivが偽
        new_swimmer = Swimmer(name=self.name, sex=self.event // 100, is_indiv=False if self.relay == 5 else True)
        if new_swimmer.is_indiv:
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
    is_indiv = db.Column(db.Boolean)


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
    notify_line(f"目標大会をセット。{target_meets_ids[0]}から{target_meets_ids[-1]}。{len(target_meets_ids)}大会の全記録調査開始")
    record_length = 0 # 追加した行数
    erased = 0 # 削除した行数
    skipped = 0 # 飛ばした種目数
    events_count = 0 # 対象の種目数

    for meet_id in Takenoko(target_meets_ids):
        events_list = scraper.all_events(meet_id) # Eventインスタンスのリスト
        events_count += (sub_total := len(events_list))

        # 既にDBにある同一大会IDの記録を抽出し、それぞれの種目IDが何行あるかを調べる
        # しかしこれでは記録数変わらずに記録の中身（タイムが空白からアップデートされたとき）に対応できない
        existing_records_in_meet = session.query(Record.event).filter_by(meet_id=meet_id).all()
        existing_event_id_list = [e.event for e in existing_records_in_meet]

        for event in events_list:
            event.crawl()
            # print(f'{event.event_id} / {sub_total} in {event.meet_id}')
            if existing_event_id_list.count(event.event_id) != len(event.rows): # 記録数が一致していなかったら削除して登録し直し
                erased += session.query(Record).filter_by(meet_id=event.meet_id, event=event.event_id).delete()
                records = [Record(*args) for args in event.parse_table()]
                for rc in records:
                    rc.set_team()
                    rc.set_swimmer()
                session.add_all(records)
                record_length += len(records)
                session.commit()
            else:
                skipped += 1

    notify_line(f'{erased}件を削除 {record_length}件を新規に保存 現在：{format(count_records(), ",")}件\n{events_count}種目中{skipped}をスキップ')


def add_meets(year, force=False):
    notify_line(f"各地域の大会情報の収集を開始。対象:20{year}年")
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

def imperfect_meets(target_meets_ids):
    # 予選種目だけ既に追加されていて、決勝種目がhtml上にない場合がある
    # 開催種目内に1種目でもタイム合計値が0の種目がある場合、当該大会はまだ結果をアップし終わってないとする
    q = session.query(
            Record.meet_id,
            # Record.event,
        ).filter(
            Record.meet_id == Meet.meet_id,
            Meet.meet_id.in_(target_meets_ids),
            ~Record.rank.in_(['失格','失格1泳者','失格2泳者','失格3泳者','失格4泳者','棄権','途中棄権'])
        ).group_by(
            Record.meet_id,
            Record.event
        ).having(
            func.sum(Record.time) == 0
        ).distinct(
            Record.meet_id
        ).all()

    return [rc.meet_id for rc in q]

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
    if not_up_to_date := imperfect_meets(target_meets_ids):
        count = session.query(Record).filter(Record.meet_id.in_(not_up_to_date)).delete(synchronize_session = False)
        session.commit()
        notify_line(f'大会ID:{not_up_to_date}、記録未納の可能性あり。{count}件の記録を削除')
    # この時点でスクレイピングが必要な大会がわかるから、既にデータを追加した大会をtargetから省いてもいいのでは？
    add_records(target_meets_ids)
    add_first_swimmer_in_relay(target_meets_ids)


def add_first_swimmer_in_relay(target_meets_ids):
    # 対象大会内のレコードに一つも1泳者のレコード（relay=1）がなかったらまだ未追加
    notify_line(f"リレー第一泳者の記録の追加を開始")
    record_length = 0 # 追加した行数
    skipped = 0 # 飛ばした種目数

    for meet_id in Takenoko(target_meets_ids, 50):
        first_swimmers = session.query(Record.record_id).filter_by(meet_id=meet_id, relay=1).all()
        if first_swimmers:
            skipped += 1 # その大会においては既に1泳者追加していた
        else:
            relay_results = session.query(
                    Record.record_id,
                    Record.event,
                    Swimmer.name,
                    Record.rank,
                    Record.team_id,
                    Record.laps
                ).filter(
                    Record.meet_id == meet_id,
                    Record.swimmer_id == Swimmer.swimmer_id,
                    Record.relay == 5,
                    ~Record.rank.in_(['失格','失格1泳者','棄権','途中棄権'])
                    # 2~4泳者の失格はよい あと失格、は誰が失格なのかわからないから一応除外
                ).all()
            only_relay_but_add = []
            sub_count = 0
            for relay in relay_results:
                swimmers = relay.name.split(',')
                if len(swimmers) == 1:
                    notify_line(f'R1_INVALID: 無効なリレーオーダー({swimmers}) on {relay.record_id}')
                    continue
                first = swimmers[0]
                # とりあえず同じ名前の人探す
                candidates = session.query(Swimmer.swimmer_id).filter_by(name=first).all()

                if candidates:
                    # 同一大会内の個人種目でその人が出場しているか
                    candidates_in_same_meet = session.query(
                            Record.swimmer_id
                        ).filter(
                            Record.meet_id == meet_id,
                            Record.swimmer_id.in_([c.swimmer_id for c in candidates])
                        ).distinct(
                            Record.swimmer_id # 同姓同名の選手が同じ大会に出場していたらオワオワリ
                        ).all()
                    suggest_s_ids = [s.swimmer_id for s in candidates_in_same_meet]

                    if (length := len(suggest_s_ids)) == 1:
                        # これは特定余裕 同じ大会内で同じ名前の選手が一人だけいた
                        sub_count += add_row_for_relay(relay, meet_id, suggest_s_ids[0])

                    elif length == 0:
                        # 同一大会で出場なし
                        if len(candidates) == 1:
                            sub_count += add_row_for_relay(relay, meet_id, candidates[0])
                            only_relay_but_add.append(f'{first}, {relay.record_id}')
                        else:
                            notify_line(f'R1_INVALID: リレーのみ出場同姓同名({first}) on {relay.record_id}')
                    else:
                        # 同姓同名が同一大会で出場したため、リレー一泳が誰か特定不可
                        notify_line(f'R1_INVALID: 同一大会内同姓同名({first}) on {relay.record_id}')

                else: # 同じ名前の人がSwimmerテーブルに存在しない
                    notify_line(f'R1_INVALID: テーブルに存在しない名前({first}) on {relay.record_id}')

            if only_relay_but_add:
                msg = ' '.join(only_relay_but_add)
                notify_line(f'{meet_id}において、リレーのみ出場の選手かつ同姓同名なしで問題なしとしたのが以下。{msg}')
            session.commit()
            print(f'{meet_id}にて{sub_count}件追加')
            record_length += sub_count

    notify_line(f'{record_length}件の第一泳者の記録を新規に保存。{skipped}大会をスキップ')

def add_row_for_relay(relay, meet_id, swimmer_id):
    event = convert_relay_event(relay.event)
    laps_list = relay.laps.split(',')
    if (lap_len:=len(laps_list)) < 4:
        notify_line(f'R1_INVALID: 無効なタイム({laps_list}) on {relay.record_id}')
        return 0
    else:
        assert lap_len % 4 == 0
        first_range = lap_len // 4
        first_laps = laps_list[:first_range]
        time = int(first_laps[-1]) # 最後の一つが１泳の正式タイム
        laps = ','.join(first_laps)
        first_result = Record(meet_id=meet_id, event=event, relay=1, rank=relay.rank, name='', team='', grade=0, time=time, laps=laps)
        first_result.swimmer_id = swimmer_id
        first_result.team_id = relay.team_id
        session.add(first_result)
        return 1

def convert_relay_event(event):
    sex = event // 100
    relay_style = (event // 10) % 10
    relay_distance = event % 10

    if relay_style == 6: #FR
        style = 1
    elif relay_style == 7: #MR
        style = 2

    if 3 <= relay_distance <= 6:
        distance = relay_distance - 2

    return sex*100 + style*10 + distance


def routine(year=None, date_min=None, date_max=None):
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
# ::Base.metadata.drop_all(bind=engine)
# ::Base.metadata.create_all(bind=engine)
# ::initialize_stats_table()
    args = sys.argv
    if len(args) == 1:
        routine()
        # add_first_swimmer_in_relay([1418201])
        # print(convert_relay_event(178))
    else:
        target = args[1]
        if target == 'meets':
            add_meets(int(args[2]))
        elif target == 'routine':
            routine(date_min=int(args[2]), date_max=int(args[3]))
        elif target == 'relay':
            target_meets = session.query(
                    Meet.meet_id
                ).filter(
                    Meet.start >= 20190400,
                    Meet.start <= 20200999
                ).order_by(
                    Meet.start,
                    Meet.meet_id
                ).all()
            target_meets_ids = [m.meet_id for m in target_meets]
            add_first_swimmer_in_relay(target_meets_ids)
        else:
            print(args)
