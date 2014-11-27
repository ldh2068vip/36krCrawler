package org.core;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class MainClass {
	// private static Hashtable alllinks = new Hashtable<String, String>();
	private static File file = new File("urls");
	private static File datafile = new File("data");
	// private static Set fetchset = new LinkedHashSet<String>();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String today = sdf.format(new Date());

	public static void main(String[] args) {
		// 建表
		try {
			Connection conn = ConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();
			stmt.execute("drop table urls if exists");
			stmt.execute("drop table article if exists");
			stmt.execute("CREATE TABLE urls(ID INT IDENTITY  PRIMARY KEY, url VARCHAR(255) unique,stat int not null)");
			stmt.execute("CREATE TABLE article(ID INT IDENTITY  PRIMARY KEY, url VARCHAR(255) unique,stat int not null)");
			stmt.execute("commit");
			ConnectionFactory.close(stmt);
			ConnectionFactory.close(conn);

		} catch (SQLException e) {
			System.err.println(e.getMessage());
		}

		Crawler crawler = new Crawler();
		crawler.start();
		fetchUrls("http://www.36kr.com/");
		// UrlCrawler ucrawler= new UrlCrawler();
		// ucrawler.run();
		// fetchData("http://www.36kr.com/p/207831.html");//test........
	}

	private static void fetchUrls(String url) {
		// System.out.println(fetchset.size());
		// System.out.println(alllinks.size());
		Document doc = null;
		try {
			doc = Jsoup.connect(url).get();
		} catch (IOException e) {
			System.err.println(e.getMessage());
			// fetchUrls((String) fetchset.iterator().next());
			try {
				Connection conn = ConnectionFactory.getConnection();
				Statement stmt = conn.createStatement();
				ResultSet res = stmt
						.executeQuery("select url from urls where stat=0 limit 1");
				res.next();
				String newurl = res.getString(1);
				stmt.execute("update urls set stat=1 where url='" + newurl
						+ "'");
				ConnectionFactory.close(stmt);
				ConnectionFactory.close(conn);
				fetchUrls(newurl);

			} catch (SQLException e1) {
				System.err.println(e1.getMessage());
			}
		}

		Elements alist = doc.select("a");
		for (Element element : alist) {
			String href = element.absUrl("href");
			String title = element.text();

			String regEx = "http://www.36kr.com/p/\\d+\\.html";
			Pattern p = Pattern.compile(regEx);
			Matcher m = p.matcher(href);

			String regEx2 = "http://www.36kr.com/.*";
			Pattern p2 = Pattern.compile(regEx2);
			Matcher m2 = p2.matcher(href);
			if (m2.matches()) {
				// fetchset.add(href);
				try {
					Connection conn = ConnectionFactory.getConnection();
					Statement stmt = conn.createStatement();
					stmt.execute("insert into urls(url,stat) values('" + href
							+ "',0)");
					ConnectionFactory.close(stmt);
					ConnectionFactory.close(conn);

				} catch (SQLException e) {
					if (e.getErrorCode() != 23505) {
						System.err.println(e.getMessage());
					}

				}

			}
			if (m.matches()) {
				// if (!alllinks.containsKey(href)) {
				// try {
				// // FileUtils.writeStringToFile(file, href + "\t" + title
				// // + "\n", true);
				// fetchData(href);
				// } catch (IOException e) {
				// e.printStackTrace();
				// }
				// }
				// alllinks.put(href, title);
				try {
					Connection conn = ConnectionFactory.getConnection();
					Statement stmt = conn.createStatement();
					stmt.execute("insert into article(url,stat) values('"
							+ href + "',0)");
					ConnectionFactory.close(stmt);
					ConnectionFactory.close(conn);

				} catch (SQLException e) {
					if (e.getErrorCode() != 23505) {
						System.err.println(e.getMessage());
					}

				}

			}
		}
		// String newurl = (String) fetchset.iterator().next();
		// fetchset.remove(newurl);
		try {
			Connection conn = ConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet res = stmt
					.executeQuery("select url from urls where stat=0 limit 1");
			res.next();
			String newurl = res.getString(1);
			stmt.execute("update urls set stat=1 where url='" + newurl + "'");
			ConnectionFactory.close(stmt);
			ConnectionFactory.close(conn);
			fetchUrls(newurl);

		} catch (SQLException e) {
			System.err.println(e.getMessage());
		}

		// fetchUrls(newurl);
	}

	static class Crawler extends Thread {

		@Override
		public void run() {
			while (true) {
				fetchData();

			}

		}
	}

	private synchronized static void fetchData() {
		try {
			Connection conn = ConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet res = stmt
					.executeQuery("select url from article where stat=0 limit 1");
			res.next();
			String url = res.getString(1);
			stmt.execute("update article set stat=1 where url='" + url + "'");
			ConnectionFactory.close(stmt);
			ConnectionFactory.close(conn);
			Document doc = null;
			doc = Jsoup.connect(url).get();
			String title = doc.title();
			Document data = Jsoup.parse(doc.html());
			String author = data.select("div.single-post__postmeta")
					.select("a").text();
			String article = doc.select("section.article").text();
			String time = doc.select("meta[name=weibo: article:create_at]")
					.get(0).attr("content");
			Pattern p = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}).*");
			Matcher m = p.matcher(time);
			if (time != null && time != "" && m.matches()) {
				time = m.group(1);
			} else {
				time = today;
			}
			FileUtils.writeStringToFile(
					datafile,
					url + "#" + title.replaceAll("#|\r|\n", "") + "#"
							+ author.replaceAll("#|\r|\n", "") + "#" + time
							+ "#" + article.replaceAll("\r|\n", "") + "\n",
					true);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		} catch (SQLException e) {
			if (e.getErrorCode() != 23505) {
				System.err.println(e.getMessage());
			}
			if (e.getErrorCode() == 2000) {

				try {
					Thread.sleep(50000);
				} catch (InterruptedException e1) {
					System.err.println(e1.getMessage());
				}
			}

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
