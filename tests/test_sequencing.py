import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from ingester.sequencing import Sequencer


def test_같은_밀리초_체결은_일련번호가_증가한다():
    """체결ID가 없으므로 동일 ms 내 구분은 이 번호가 유일한 수단이다."""
    s = Sequencer()
    assert s.assign("005930", 1000)[0] == 0
    assert s.assign("005930", 1000)[0] == 1
    assert s.assign("005930", 1000)[0] == 2


def test_밀리초가_바뀌면_초기화된다():
    s = Sequencer()
    s.assign("005930", 1000)
    s.assign("005930", 1000)
    assert s.assign("005930", 1001)[0] == 0


def test_종목별로_독립적이다():
    s = Sequencer()
    s.assign("005930", 1000)
    s.assign("005930", 1000)
    assert s.assign("000660", 1000)[0] == 0


def test_역행_타임스탬프를_감지한다():
    """소스가 순서를 보장하지 않으면 워터마크 지연 허용치가 달라진다."""
    s = Sequencer()
    assert s.assign("005930", 2000)[1] is False
    assert s.assign("005930", 2001)[1] is False
    assert s.assign("005930", 1999)[1] is True   # 역행
    # 역행 후에도 최대값 기준은 유지된다
    assert s.assign("005930", 1998)[1] is True


def test_첫_체결은_역행이_아니다():
    s = Sequencer()
    assert s.assign("005930", 500)[1] is False
