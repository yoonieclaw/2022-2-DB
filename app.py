from flask import Flask, render_template, session, request, jsonify, Response
import db


app = Flask(__name__)
app.config.from_mapping(
    SECRET_KEY = 'dev',
)

@app.route('/')
def index():
    return render_template("home.html")


# ===================================================
# 테이블 검색 API
# ===================================================
@app.route('/searchtable')
def searchtable():
    """
    REQUEST URL: '/searchtable'
        - Method: GET
        - Parameters:
            - tname: 테이블 명
            - jkey: 대표 결합키 이름
            - rattr: 대표 속성 이름
            - attr: 속성명          -> 결합 시 테이블 검색에 사용하기 위해 추가했습니다. 2022.12.01
            - scan: 스캔 완료 여부

        - ex: GET '/localhost:5000/searchtable?scan=F&tname=1_Fitness_Measurement'

    RESPONSE: application/json
        [
            {
                "attrs": [
                    "AGE_GBN",
                    "CENTER_NM",
                    "CERT_GBN",
                    ...
                ],
                "join_keys": [],
                "records": 1200,
                "repr_attrs": [],
                "table_name": "1_Fitness_Measurement"
            }
        ]
    """
    if 'database' in session:
        table_name = request.args.get('tname', '')
        repr_attr_name = request.args.get('rattr', '')
        attr_name = request.args.get('attr', '') #변경
        join_key = request.args.get('jkey', '')
        is_scanned = request.args.get('scan', 'T') if request.args.get('scan', '') in ("T", "F") else ''

        conn = db.get_db()
        with conn:
            cur = conn.cursor()
            table_list_stmt = f'SELECT `t`.`table_name` FROM TABLE_LIST AS `t` WHERE `t`.`is_scanned` = "{is_scanned}"'

            if table_name:
                table_list_stmt += f' AND `t`.`table_name` LIKE "%{table_name}%"'

            if repr_attr_name:
                table_list_stmt += f' AND `t`.`table_name` IN \
                                        (SELECT `r`.`table_name` FROM REPR_ATTR AS `r` \
                                        JOIN STD_REPR_ATTR AS `s` ON `r`.`repr_attr_id` = `s`.`repr_attr_id` \
                                        WHERE `s`.`repr_attr_name` = "{repr_attr_name}")'

            if attr_name: #변경
                table_list_stmt += f' AND `t`.`table_name` IN\
                                        (SELECT `a`.`table_name` FROM ATTR AS `a`\
                                        WHERE `a`.`attr_name` LIKE "%{attr_name}%")'

            if join_key:
                table_list_stmt += f' AND `t`.`table_name` IN \
                                        (SELECT `j`.`table_name` FROM JOIN_KEY AS `j` \
                                        JOIN STD_JOIN_KEY AS `s` ON `j`.`join_key_id` = `s`.`key_id` \
                                        WHERE `s`.`key_name` = "{join_key}")'
            
            cur.execute(table_list_stmt)
            all_table_list = []
            for table_name in cur.fetchall():

                # 모든 속성, 레코드 수
                attr_list_stmt = f'SELECT `attr_name`, `record_count` FROM `ATTR` WHERE `table_name` = "{table_name[0]}"'
                cur.execute(attr_list_stmt)
                attrs = []
                records = 0
                for attr_name, record_count in cur.fetchall():
                    attrs.append(attr_name)
                    records = record_count

                # 모든 대표 속성
                repr_attr_stmt = f'SELECT `s`.`repr_attr_name` FROM REPR_ATTR AS `r` \
                                    JOIN STD_REPR_ATTR AS `s` ON `s`.`repr_attr_id` = `r`.`repr_attr_id` \
                                    WHERE `r`.`table_name` = "{table_name[0]}"'
                if repr_attr_name:
                    repr_attr_stmt += f' AND `s`.`repr_attr_name` = "{repr_attr_name}"'
                cur.execute(repr_attr_stmt)
                repr_attrs = []
                for attr_name in cur.fetchall():
                    repr_attrs.append(attr_name)

                # 모든 대표 결합키
                join_key_stmt = f'SELECT `s`.`key_name` FROM JOIN_KEY AS `j` \
                                    JOIN STD_JOIN_KEY AS `s` ON `s`.`key_id` = `j`.`join_key_id` \
                                    WHERE `j`.`table_name` = "{table_name[0]}"'
                if join_key:
                    join_key_stmt += f' AND `s`.`key_name` = "{join_key}"'
                cur.execute(join_key_stmt)
                join_keys = []
                for key_name in cur.fetchall():
                    join_keys.append(key_name)

                all_table_list.append(
                    {"table_name": table_name[0], "attrs": attrs, "records": records, "repr_attrs": repr_attrs, "join_keys": join_keys})
        return jsonify(all_table_list)
    else:
        return Response(status=401, mimetype='application/json')


# ===================================================
# 대표 속성 목록 API
# ===================================================
@app.route('/stdattr')
def stdattr():
    """
    REQUEST URL: '/stdattr'
        - Method: GET
        - Parameters:
            - None

    RESPONSE: application/json
        [
            "학업정보",
            "금융정보",
            "회원정보",
            "건강정보"
        ]
    """
    if 'database' in session:
        conn = db.get_db()
        with conn:
            cur = conn.cursor()
            cur.execute('SELECT `repr_attr_name` FROM `STD_REPR_ATTR`')
            result = []
            for attr in cur.fetchall():
                result.append(attr[0])
            return jsonify(result)
    else:
        return Response(status=401, mimetype='application/json')


# ===================================================
# 대표 결합키 목록 API
# ===================================================
@app.route('/stdkey')
def stdkey():
    """
    REQUEST URL: '/stdkey'
        - Method: GET
        - Parameters:
            - None

    RESPONSE: application/json
        [
            "주민등록번호",
            "전화번호",
            "차량번호",
            "이메일",
            "IP"
        ]
    """
    if 'database' in session:
        conn = db.get_db()
        with conn:
            cur = conn.cursor()
            cur.execute('SELECT `key_name` FROM `STD_JOIN_KEY`')
            result = []
            for key_name in cur.fetchall():
                result.append(key_name[0])
            return jsonify(result)
    else:
        return Response(status=401, mimetype='application/json')


from routes import dblogin, tablescan, tablerevise, singlejoin, multiplejoin, result, preferences

app.register_blueprint(dblogin.bp)
app.register_blueprint(tablescan.bp)
app.register_blueprint(tablerevise.bp)
app.register_blueprint(singlejoin.bp)
app.register_blueprint(multiplejoin.bp)
app.register_blueprint(result.bp)
app.register_blueprint(preferences.bp)


if __name__ == '__main__':
    app.run(debug=True)