import pytest
import pandas as pd
import os

FILE_PATH = "data/processed/cleaned_data.csv"


def test_file_exists():
    """1. 결과 파일이 실제로 생성되었는지 테스트"""
    assert os.path.exists(
        FILE_PATH
    ), "❌ 전처리된 파일이 없습니다. ETL을 먼저 실행하세요."


def test_leakage_removed():
    """2. Data Leakage 컬럼(gold_earned)이 확실히 삭제되었는지 테스트"""
    if not os.path.exists(FILE_PATH):
        pytest.skip("파일이 없어 테스트 건너뜀")

    df = pd.read_csv(FILE_PATH)
    assert (
        "gold_earned" not in df.columns
    ), "❌ Data Leakage! 'gold_earned' 컬럼이 남아있습니다."


def test_no_missing_values():
    """3. KNN으로 결측치가 모두 채워졌는지 테스트"""
    if not os.path.exists(FILE_PATH):
        pytest.skip("파일이 없어 테스트 건너뜀")

    df = pd.read_csv(FILE_PATH)

    numeric_cols = df.select_dtypes(include=["number"]).columns
    assert (
        df[numeric_cols].isnull().sum().sum() == 0
    ), "❌ 결측치(NaN)가 아직 남아있습니다."


def test_data_volume():
    """4. 데이터가 너무 적지는 않은지(최소 10개 이상) 테스트"""
    if not os.path.exists(FILE_PATH):
        pytest.skip("파일이 없어 테스트 건너뜀")

    df = pd.read_csv(FILE_PATH)
    assert len(df) > 10, f"❌ 데이터가 너무 적습니다. (현재 {len(df)}행)"
