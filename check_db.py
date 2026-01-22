import os
import sqlite3

import pandas as pd


def check_database():
    # 1. DB 파일 위치 (load.py에서 설정한 경로)
    db_path = "data/lol_data.db"

    # 파일이 실제로 있는지부터 확인
    if not os.path.exists(db_path):
        print(f"실패: DB 파일이 없습니다! ({db_path})")
        return

    print(f"DB 파일 발견: {db_path}")

    # 2. DB 연결
    try:
        conn = sqlite3.connect(db_path)

        # 3. 데이터 조회 (SQL 쿼리)
        query = "SELECT * FROM challenger_stats"
        df = pd.read_sql(query, conn)

        conn.close()

        # 4. 결과 출력
        print("\n" + "=" * 40)
        print(f"저장된 데이터 개수: {len(df)}개")
        print("=" * 40)

        if len(df) > 0:
            print("\n상위 5개 데이터 미리보기:")
            print(df.head().to_string())  # 예쁘게 출력
            print("\n검증 완료: 데이터가 정상적으로 적재되었습니다.")
        else:
            print("\n경고: 테이블은 있지만 데이터가 0개입니다.")

    except Exception as e:
        print(f"\n❌ DB 읽기 실패: {e}")


if __name__ == "__main__":
    check_database()
