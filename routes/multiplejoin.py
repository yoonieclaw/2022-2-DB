from flask import Blueprint, render_template, session, redirect, url_for, request, current_app
import db


bp = Blueprint('multiplejoin', __name__, url_prefix='/multiplejoin', template_folder='templates')

# ===================================================
# source 테이블 검색
# ===================================================
@bp.route('/', methods=['GET'])
def source_table_search():
    if 'database' in session:
        return render_template("multiplejoin_source.html")
    else:
        return redirect(url_for('dblogin.dblogin'))

# ===================================================
# target 테이블 검색
# ===================================================
@bp.route('/<source_table_name_jk>', methods=['GET', 'POST'])
def target_table_search(source_table_name_jk):
    tabledname = source_table_name_jk.split('+')[0]
    joinkey = source_table_name_jk.split('+')[1]

    conn = db.get_db()
    with conn:
        cur = conn.cursor()

        cur.execute('SELECT record_count FROM attr WHERE table_name = %s', [tabledname])
        total_record = cur.fetchall()[0][0]

        # 대표속성
        reprattr = []

        cur.execute('SELECT repr_attr_name FROM repr_attr, std_repr_attr WHERE repr_attr.repr_attr_id = std_repr_attr.repr_attr_id AND table_name = %s', [tabledname])
        for repr_attr_name in cur.fetchall():
            reprattr.append(repr_attr_name)

        fin_reprattr = ''

        if len(reprattr) == 0:
            fin_reprattr = '-'
        else:
            reprattr = set(reprattr)
            if len(list(reprattr)) == 1:
                for i in range(0, len(list(reprattr))):
                    fin_reprattr = fin_reprattr + str(list(reprattr)[i][0])
            else:
                for i in range(0, len(list(reprattr))):
                    fin_reprattr = fin_reprattr + str(list(reprattr)[i][0]) + ', '
            fin_reprattr = fin_reprattr[:-2]

    
    return render_template(
        'multiplejoin_target.html',
        table_name = tabledname,
        total_record = total_record,
        repr_attr = fin_reprattr,
        join_key = joinkey
    )
# ===================================================
# 다중 결합 결과
# ===================================================
@bp.route('/<source_table_name_jk>/<target_tables>', methods=['GET', 'POST'])
def multiple_result(source_table_name_jk, target_tables):
    """
    join_id: 결합 아이디
    in_id: 내부 아이디(다중 결합 안의 아이디)
    table_A: 소스 테이블 이름
    table_A_rec_count: 소스 테이블 레코드 수
    table_A_attr: 소스 결합키 속성
    table_B: 타겟 테이블 이름
    table_B_rec_count: 타겟 테이블 레코드 수
    table_B_attr: 타겟 결합키 속성
    joinkey: 대표 결합키
    joined_rec_num = 결합 레코드 수
    source_success = 결합 성공률(W1)
    target_success = 결합 성공률(W2)
    finished: 0: 진행중 1: 완료
    join_stat: 결합 진행 상황 (표시)

    이 파일에서는 결합 진행 할 테이블을 리스트에 올리고, 결과 조회 창 진입하면 result.py
    에서 결합 진행
    """
    # db 연결
    conn = db.get_db()
    with conn:
        cur = conn.cursor()


        table_A = source_table_name_jk.split('+')[0]
        joinkey = source_table_name_jk.split('+')[1]
        table_B_list = target_tables.split('+')
        table_B_list.remove('')
        target_selected = len(table_B_list)
        cur.execute('SELECT MAX(id) FROM MULTIPLE_JOIN_TABLE_LIST')
        max_id = cur.fetchall()[0][0]
        if max_id is None:
            join_id = 1
        else:
            join_id = max_id+1

        for target in range(target_selected):
            table_B = table_B_list[target]
            joined_table = f'다중결합테이블_{join_id}_{target+1}'
            cur.execute(f'SELECT key_id FROM std_join_key where key_name = "{joinkey}"')
            join_key_id = cur.fetchall()[0][0]
            cur.execute(
                f'SELECT attr_name FROM JOIN_KEY WHERE table_name = "{table_A}" AND join_key_id = {join_key_id}')
            table_A_attr = cur.fetchall()[0][0]

            cur.execute(
                f'SELECT attr_name FROM JOIN_KEY WHERE table_name = "{table_B}" AND join_key_id = {join_key_id}')
            table_B_attr = cur.fetchall()[0][0]

            cur.execute('SELECT record_count FROM attr WHERE table_name = %s', [table_A])
            table_A_rec_count = cur.fetchall()[0][0]
            cur.execute('SELECT record_count FROM attr WHERE table_name = %s', [table_B])
            table_B_rec_count = cur.fetchall()[0][0]

            joined_rec_num = 0
            source_success = 0
            target_success = 0
            finished = 0
            join_stat = f'진행중({target+1}/{target_selected})'

            # 결합 테이블 리스트에 입력

            cur.execute(
                f'INSERT INTO MULTIPLE_JOIN_TABLE_LIST VALUES ({join_id}, {target+1}, "{table_A}", {table_A_rec_count}, "{table_A_attr}", "{table_B}", {table_B_rec_count}, "{table_B_attr}", "{joined_table}", "{joinkey}", {joined_rec_num}, {source_success}, {target_success}, {finished}, "{join_stat}")')
            conn.commit()

    return render_template('multiplejoin_result.html')
