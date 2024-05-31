from flask import Blueprint, render_template, session, redirect, url_for, request, current_app
import db

bp = Blueprint('singlejoin', __name__, url_prefix='/singlejoin', template_folder='templates')


# ===================================================
# source 테이블 검색
# ===================================================
@bp.route('/', methods=['GET'])
def source_table_search():
    if 'database' in session:
        return render_template("singlejoin_source.html")
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
        'singlejoin_target.html',
        table_name = tabledname,
        total_record = total_record,
        repr_attr = fin_reprattr,
        join_key = joinkey
    )
# ===================================================
# 단일 결합 결과
# ===================================================
@bp.route('/<source_table_name_jk>/<target_table>', methods=['GET', 'POST'])
def single_result(source_table_name_jk, target_table):
    """
    join_id: 결합 아이디
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

    결합은 1초 후에 진행되며, 새로고침과 함께 결합 데이터가 표시됩니다
    """

    # db 연결
    conn = db.get_db()
    with conn:
        cur = conn.cursor()

        table_A = source_table_name_jk.split('+')[0]
        joinkey = source_table_name_jk.split('+')[1]
        table_B = target_table
        cur.execute('SELECT MAX(id) FROM SINGLE_JOIN_TABLE_LIST')
        last_id = cur.fetchall()[0][0]
        if last_id is None:
            join_id = 1
        else:
            join_id = last_id + 1
        cur.execute(f'SELECT key_id FROM std_join_key where key_name = "{joinkey}"')
        join_key_id = cur.fetchall()[0][0]

        # 테이블 리스트가 비어있을 경우
        if(join_id == 1):
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
            join_stat = "진행중"

            joined_table = f'단일결합테이블_{join_id}'

            # 결합 테이블 리스트에 입력
            cur.execute(
                f'INSERT INTO SINGLE_JOIN_TABLE_LIST VALUES ({join_id}, "{table_A}", {table_A_rec_count}, "{table_A_attr}", "{table_B}", {table_B_rec_count}, "{table_B_attr}", "{joined_table}", "{joinkey}", {joined_rec_num}, {source_success}, {target_success}, {finished}, "{join_stat}")')
            conn.commit()
            return render_template(
                'singlejoin_result.html',
                table_A=table_A,
                table_A_rec_count=table_A_rec_count,
                table_A_attr=table_A_attr,
                table_B=table_B,
                table_B_rec_count=table_B_rec_count,
                table_B_attr=table_B_attr,
                joinkey=joinkey,
                joined_rec_num=joined_rec_num,
                source_success=source_success,
                target_success=target_success,
                join_stat=join_stat,
                joined_table=joined_table,
                table_state=finished
            )

        # 원래 한 줄이었는데, 계속 에러 나서 그냥 풀어서 썼습니다
        cur.execute('SELECT source_table_name FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        table_A_name = cur.fetchall()[0][0]
        cur.execute('SELECT target_table_name FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        table_B_name = cur.fetchall()[0][0]
        cur.execute('SELECT source_record_count FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        table_A_rec_count = cur.fetchall()[0][0]
        cur.execute('SELECT target_record_count FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        table_B_rec_count = cur.fetchall()[0][0]
        cur.execute('SELECT source_attr_name FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        table_A_attr = cur.fetchall()[0][0]
        cur.execute('SELECT target_attr_name FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        table_B_attr = cur.fetchall()[0][0]
        cur.execute('SELECT joined_record_count FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        joined_rec_num = cur.fetchall()[0][0]
        cur.execute('SELECT source_success_rate FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        source_success = cur.fetchall()[0][0]
        cur.execute('SELECT target_success_rate FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        target_success = cur.fetchall()[0][0]
        cur.execute('SELECT join_status FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        join_stat = cur.fetchall()[0][0]
        cur.execute('SELECT joined_table_name FROM SINGLE_JOIN_TABLE_LIST WHERE id = %s', [last_id])
        joined_table = cur.fetchall()[0][0]



        cur.execute(f'SELECT is_complete FROM SINGLE_JOIN_TABLE_LIST WHERE id = {last_id}')
        # table_state: 1 = 결합완료 0 = 진행중
        for i in cur.fetchall():
            table_state = i[0]
        # 결합이 완료되었거나 직전에 같은 결합을 실행한 경우 같은 페이지 띄움
        if (table_state == 1 and table_A == table_A_name and table_B == table_B_name):
            return render_template(
                'singlejoin_result.html',
                table_A=table_A,
                table_A_rec_count=table_A_rec_count,
                table_A_attr=table_A_attr,
                table_B=table_B,
                table_B_rec_count=table_B_rec_count,
                table_B_attr=table_B_attr,
                joinkey=joinkey,
                joined_rec_num=joined_rec_num,
                source_success=source_success,
                target_success=target_success,
                join_stat=join_stat,
                joined_table=joined_table,
                table_state=table_state
            )
        # 결합을 하지 않은 상태일 경우, 결합을 진행하고 변동사항 업데이트
        elif table_state == 0:
            # 결합 테이블 생성
            cur.execute(
                f'CREATE TABLE {joined_table} AS SELECT A.* FROM {table_A} AS A INNER JOIN {table_B} AS B ON A.{table_A_attr} = B.{table_B_attr}')

            joined_table = f'단일결합테이블_{last_id}'
            cur.execute(f'SELECT COUNT(*) FROM {joined_table}')
            joined_rec_num = cur.fetchall()[0][0]
            cur.execute('SELECT record_count FROM attr WHERE table_name = %s', [table_A])
            table_A_rec_count = cur.fetchall()[0][0]
            cur.execute('SELECT record_count FROM attr WHERE table_name = %s', [table_B])
            table_B_rec_count = cur.fetchall()[0][0]
            source_success = round(joined_rec_num / table_A_rec_count, 2)
            target_success = round(joined_rec_num / table_B_rec_count, 2)
            finished = 1
            join_stat = "완료"

            cur.execute(
                f'UPDATE SINGLE_JOIN_TABLE_LIST SET joined_record_count = {joined_rec_num}, source_success_rate = {source_success}, target_success_rate = {target_success}, is_complete = {finished}, join_status = "{join_stat}" WHERE id = {last_id}')
            conn.commit()

            return render_template(
                'singlejoin_result.html',
                table_A=table_A,
                table_A_rec_count=table_A_rec_count,
                table_A_attr=table_A_attr,
                table_B=table_B,
                table_B_rec_count=table_B_rec_count,
                table_B_attr=table_B_attr,
                joinkey=joinkey,
                joined_rec_num=joined_rec_num,
                source_success=source_success,
                target_success=target_success,
                join_stat=join_stat,
                joined_table=joined_table
                )

        # 결합 리스트의 모든 테이블들이 결합 완료, 선택 된 테이블이 전과 다를 경우 결합 리스트에 등록
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
        join_stat = "진행중"

        joined_table = f'단일결합테이블_{join_id}'

        # 결합 테이블 리스트에 입력
        cur.execute(f'INSERT INTO SINGLE_JOIN_TABLE_LIST VALUES ({join_id}, "{table_A}", {table_A_rec_count}, "{table_A_attr}", "{table_B}", {table_B_rec_count}, "{table_B_attr}", "{joined_table}", "{joinkey}", {joined_rec_num}, {source_success}, {target_success}, {finished}, "{join_stat}")')
        conn.commit()
    return render_template(
        'singlejoin_result.html',
        table_A=table_A,
        table_A_rec_count=table_A_rec_count,
        table_A_attr=table_A_attr,
        table_B=table_B,
        table_B_rec_count=table_B_rec_count,
        table_B_attr=table_B_attr,
        joinkey=joinkey,
        joined_rec_num=joined_rec_num,
        source_success=source_success,
        target_success=target_success,
        join_stat=join_stat,
        joined_table=joined_table,
        table_state=finished
    )
