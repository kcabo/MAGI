class FormatEvent:
    def __init__(self, event_code): # 112
        self.code = event_code
        self.style = (event_code % 100) // 10
        distances = [0, 25, 50, 100, 200, 400, 800, 1500]
        self.distance = distances[event_code % 10]

    def jpn_event(self):
        styles = ['', '自由形', '背泳ぎ', '平泳ぎ', 'バタフライ', '個人メドレー']
        style = styles[self.style]
        if style:
            return f'{self.distance}m {style}'
        else:
            return '-'

    def jpn_style(self):
        styles = ['', '自由形', '背泳ぎ', '平泳ぎ', 'バタフライ', '個人メドレー']
        return styles[self.style]

    def eng_style(self):
        eng_styles = ['', 'Fr', 'Ba', 'Br', 'Fly', 'IM']
        return eng_styles[self.style]

    def eng_event(self):
        eng_styles = ['', 'Fr', 'Ba', 'Br', 'Fly', 'IM']
        return f'{self.distance}{eng_styles[self.style]}'



area_dict = {
    1: "北海道",
    2: "青森",
    3: "岩手",
    4: "宮城",
    5: "秋田",
    6: "山形",
    7: "福島",
    8: "茨城",
    9: "栃木",
    10: "群馬",
    11: "埼玉",
    12: "千葉",
    13: "東京",
    14: "神奈川",
    15: "山梨",
    16: "長野",
    17: "新潟",
    18: "富山",
    19: "石川",
    20: "福井",
    21: "静岡",
    22: "愛知",
    23: "三重",
    24: "岐阜",
    25: "滋賀",
    26: "京都",
    27: "大阪",
    28: "兵庫",
    29: "奈良",
    30: "和歌山",
    31: "鳥取",
    32: "島根",
    33: "岡山",
    34: "広島",
    35: "山口",
    36: "香川",
    37: "徳島",
    38: "愛媛",
    39: "高知",
    40: "福岡",
    41: "佐賀",
    42: "長崎",
    43: "熊本",
    44: "大分",
    45: "宮崎",
    46: "鹿児島",
    47: "沖縄",
    48: "学連関東",
    49: "学連中部",
    50: "学連関西",
    51: "学連中・四国",
    52: "学連九州",
    53: "学連北部",
    70: "全国大会",
    80: "国際大会"
}


japanese_grades = [
    "これは0番目",
    "小学1",
    "小学2",
    "小学3",
    "小学4",
    "小学5",
    "小学6",
    "中学1",
    "中学2",
    "中学3",
    "高校1",
    "高校2",
    "高校3",
    "大学1",
    "大学2",
    "大学3",
    "大学4",
    "大学5",
    "大学6",
    "一般",
]

foreign_teams = [
    'ﾄﾝｶﾞ',
    'ﾈﾊﾟｰﾙ',
    'ｶﾝﾎﾞｼﾞｱ',
    'AUS',
    'Australia',
    'AUT',
    'BEL',
    'BLR',
    'BRA',
    'CAN',
    'CHN',
    'DEN',
    'ESP',
    'FAR',
    'GER',
    'HKG',
    'HUN',
    'ITA',
    'JAM',
    'KOR',
    'LTU',
    'NZL',
    'OMA',
    'QAT',
    'RSA',
    'RUS',
    'SGP',
    'SWE',
    'THA',
    'Thailand',
    'TPE',
    'UKR',
    'USA',
    'HUNGARY',
    'ISRAEL',
    'KOREA',
    'THAILAND',
    'MEXICO',
]
