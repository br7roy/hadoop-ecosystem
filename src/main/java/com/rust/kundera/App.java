package com.rust.kundera;

import entity.T1;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * Hello world!
 */
public class App {
	public static void main(String[] args) {
		EntityManagerFactory hbase_pu = Persistence.createEntityManagerFactory("hbase_pu");
		EntityManager entityManager = hbase_pu.createEntityManager();
		T1 t1 = new T1();
		t1.setAge(80);
		t1.setNo("001");
		t1.setName("tomasLee");
		entityManager.persist(t1);

	}
}
