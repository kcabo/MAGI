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


# styles = {
#     1:"自由形",
#     2:"背泳ぎ",
#     3:"平泳ぎ",
#     4:"バタフライ",
#     5:"個人メドレー",
#     6:"フリーリレー",
#     7:"メドレーリレー"
# }
#
# distances = {
#     1:"25m",
#     2:"50m",
#     3:"100m",
#     4:"200m",
#     5:"400m",
#     6:"800m",
#     7:"1500m"
# }
#
# sex = {
#     1:"男子",
#     2:"女子",
#     3:"混合"
# }

# style_2_num = {
#     'Fr': 1,
#     'Ba': 2,
#     'Br': 3,
#     'Fly':4,
#     'IM': 5,
#     'FR': 6,
#     'MR': 7
# }
# style_2_japanese = {
#     'Fr': '自由形',
#     'Ba': '背泳ぎ',
#     'Br': '平泳ぎ',
#     'Fly': 'バタフライ',
#     'IM': '個人メドレー',
#     'FR': 'フリーリレー',
#     'MR': 'メドレーリレー',
# }
#
# distance_2_num = {
#     25:1,
#     50:2,
#     100:3,
#     200:4,
#     400:5,
#     800:6,
#     1500:7
# }

# event_2_num = {
#     '50Fr': {'style': 1,'distance': 2},
#     '100Fr': {'style': 1,'distance': 3},
#     '200Fr': {'style': 1,'distance': 4},
#     '400Fr': {'style': 1,'distance': 5},
#     '800Fr': {'style': 1,'distance': 6},
#     '1500Fr': {'style': 1,'distance': 7},
#     '50Ba': {'style': 2,'distance': 2},
#     '100Ba': {'style': 2,'distance': 3},
#     '200Ba': {'style': 2,'distance': 4},
#     '50Br': {'style': 3,'distance': 2},
#     '100Br': {'style': 3,'distance': 3},
#     '200Br': {'style': 3,'distance': 4},
#     '50Fly': {'style': 4,'distance': 2},
#     '100Fly': {'style': 4,'distance': 3},
#     '200Fly': {'style': 4,'distance': 4},
#     '100IM': {'style': 5,'distance': 3},
#     '200IM': {'style': 5,'distance': 4},
#     '400IM': {'style': 5,'distance': 5}
# }

# area_list = [
#     "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16",
#     "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32",
#     "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48",
#     "49", "50", "51", "52", "53", "70", "80"
# ]

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

style_and_distance = [11,12,13,14,15,16,17,21,22,23,24,31,32,33,34,41,42,43,44,53,54,55,63,64,65,66,73,74,75]

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

]
