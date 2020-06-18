/*
 * Package:  tk.tak.kundera
 * FileName: TestCRUD
 * Author:   Tak
 * Date:     19/3/26 21:40
 * email:    bryroy@gmail.com
 */
package tk.tak.kundera;

import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.util.List;

/**
 * do CRUD in HBase with kundera
 *
 * @author Tak
 */
public class TestCRUD {

	private EntityManagerFactory entityManagerFactory;

	@Before
	public void init() {
		entityManagerFactory = Persistence.createEntityManagerFactory("hbase_pu");
	}

	/**
	 * 查询
	 */
	@Test
	public void testQuery() {
		EntityManager entityManager = entityManagerFactory.createEntityManager();
		Query query = entityManager.createQuery("select t.age,t.name from T1 t where t.rowKey='row1'");
		List<T1> resultList = query.getResultList();
		resultList.forEach(System.out::println);
		// T1 row1 = entityManager.find(T1.class, "row1");
		// System.out.printf("no:%s,name:%s", row1.getNo(), row1.getName());
	}

	/**
	 * 插入
	 */
	@Test
	public void testInsert() {
		EntityManager entityManager = entityManagerFactory.createEntityManager();
		T1 t1 = new T1();
		t1.setNo("row1");
		t1.setAge(80);
		t1.setName("tomasLee");
		entityManager.persist(t1);
	}

	/**
	 * 删除
	 */
	@Test
	public void delete() {
		EntityManager entityManager = entityManagerFactory.createEntityManager();
		T1 t1 = new T1();
		t1.setNo("row1");
		entityManager.remove(t1);
	}

	/**
	 * 批量插入
	 */
	@Test
	public void batchInsert() {
		EntityManager entityManager = entityManagerFactory.createEntityManager();
		entityManager.getTransaction().begin();
		for (int i = 0; i < 1000; i++) {
			T1 t = new T1();
			t.setRowKey("row" + i);
			t.setNo("no" + i);
			t.setName("tomas" + i);
			t.setAge(i % 100);
			entityManager.persist(t);
		}
		entityManager.getTransaction().commit();
	}

	/**
	 * 查询集合
	 */
	@Test
	public void findALl() {
		EntityManager entityManager = entityManagerFactory.createEntityManager();
		String jpaQl = "select t from T1 t";
		Query query = entityManager.createQuery(jpaQl);
		List<T1> list = query.getResultList();
		list.forEach(System.out::println);

	}
}
