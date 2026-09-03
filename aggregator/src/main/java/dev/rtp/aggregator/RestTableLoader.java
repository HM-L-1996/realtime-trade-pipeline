package dev.rtp.aggregator;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

/**
 * REST 카탈로그에서 Iceberg 테이블을 여는 {@link TableLoader}.
 *
 * <h2>왜 직접 구현했는가</h2>
 * Iceberg 가 제공하는 {@code TableLoader.fromCatalog(CatalogLoader.rest(...))} 는
 * 시그니처에 <b>Hadoop {@code Configuration} 을 요구한다.</b> 그런데
 * <ul>
 *   <li>Flink 배포 이미지의 {@code /opt/flink/lib} 에는 Hadoop 이 없다(직접 확인했다).
 *   <li>{@code iceberg-flink-runtime} 도 Hadoop 을 담고 있지 않다(직접 확인했다).
 * </ul>
 * 즉 그 경로를 쓰려면 fat jar 에 Hadoop 을 넣어야 하는데,
 * 이 프로젝트는 {@code aggregator/pom.xml} 에서 <b>이미 Hadoop 을 끌어오지 않기로</b>
 * 정해 두었다(S3 접근에 {@code hadoop-aws} 대신 {@code iceberg-aws-bundle} 을 쓴다).
 * 그 결정을 뒤집지 않으려고 로더를 직접 만들었다.
 *
 * <p>REST 카탈로그는 Hadoop 이 전혀 필요 없다 — {@code RESTCatalog} 는 인자 없는
 * 생성자와 {@code initialize(name, Map)} 만으로 동작한다. 필요 없는 의존성을
 * 시그니처 때문에 끌어오는 상황이었을 뿐이다.
 *
 * <h2>직렬화</h2>
 * TaskManager 로 보내져야 하므로 <b>문자열만 들고 있는다.</b>
 * 카탈로그 객체는 {@link #open()} 에서 만들고 {@code transient} 로 둔다.
 */
final class RestTableLoader implements TableLoader {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> props;
    private final String[] namespace;
    private final String tableName;

    private transient RESTCatalog catalog;

    RestTableLoader(Map<String, String> props, TableIdentifier id) {
        this.props = new HashMap<>(props);
        this.namespace = id.namespace().levels();
        this.tableName = id.name();
    }

    private TableIdentifier identifier() {
        return TableIdentifier.of(Namespace.of(namespace), tableName);
    }

    @Override
    public void open() {
        if (catalog == null) {
            RESTCatalog c = new RESTCatalog();
            c.initialize("rtp", props);
            catalog = c;
        }
    }

    @Override
    public boolean isOpen() {
        return catalog != null;
    }

    @Override
    public Table loadTable() {
        open();
        return catalog.loadTable(identifier());
    }

    @Override
    public TableLoader clone() {
        // 새 인스턴스를 돌려준다. 카탈로그는 공유하지 않는다 -
        // 서브태스크마다 자기 연결을 갖는 편이 종료 처리가 단순하다.
        return new RestTableLoader(props, identifier());
    }

    @Override
    public void close() {
        RESTCatalog c = catalog;
        catalog = null;
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                // 닫다가 실패해도 잡을 멈출 이유가 없다. 이미 쓰기는 끝났다.
            }
        }
    }
}
