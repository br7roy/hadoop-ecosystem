<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%--
  Created by IntelliJ IDEA.
  User: Rust
  Date: 19/5/18
  Time: 20:51
  To change this template use File | Settings | File Templates.
--%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%
%>
<html>
<head>

    <title>振鑫花园人工智障搜索引擎</title>
</head>
<body>
${Title}
<form action="search.do" method="post">
    <input type="hidden" value="1" name="num">
    <input type="hidden" value="0" name="count">
    <label>
        <input name="keyword" size="30"/>
    </label>&nbsp;<input type="submit" value="搜索">
</form>
<hr>
<c:if test="${! empty page.list}">
    智障搜索引擎为您找到相关结果${page.totalCount}个<br>
    <c:forEach var="bean" items="${page.list}">
        <a href="${bean.url}" target="_blank">${bean.title}</a><br>
        ${bean.content}<br>
    </c:forEach>
    <br>
    <c:forEach var="n" items="{page.numbers}">
        <a href="javascript:">${n}</a>
    </c:forEach>
</c:if>
</body>
</html>
