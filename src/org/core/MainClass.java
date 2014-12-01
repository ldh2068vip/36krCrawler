package org.core;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import sun.util.logging.resources.logging;

public class MainClass {
	// private static Hashtable alllinks = new Hashtable<String, String>();
	// private static File file = new File("urls");
	private static File datafile = new File("data");
	// private static Set fetchset = new LinkedHashSet<String>();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String today = sdf.format(new Date());
	private static Logger log = Logger.getLogger("36krCrwler");

	public static void main(String[] args) {
		// 建表
		try {
			Connection conn = ConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();
			stmt.execute("drop table urls if exists");
			stmt.execute("drop table article if exists");
			stmt.execute("CREATE TABLE urls(ID INT IDENTITY  PRIMARY KEY, url VARCHAR(400) unique,stat int not null)");
			stmt.execute("CREATE TABLE article(ID INT IDENTITY  PRIMARY KEY, url VARCHAR(255) unique,stat int not null)");
			ConnectionFactory.close(stmt);
			ConnectionFactory.close(conn);

		} catch (SQLException e) {
			log.warning(e.getMessage());
		}

		UrlCrawler ucrawler = new UrlCrawler();
		ucrawler.start();

		Crawler crawler = new Crawler();
		crawler.start();
		Crawler crawler2 = new Crawler();
		crawler2.start();
		Crawler crawler3 = new Crawler();
		crawler3.start();
		// fetchUrls("http://www.36kr.com/");

		// fetchData("http://www.36kr.com/p/207831.html");//test........
	}

	static class UrlCrawler extends Thread {

		@Override
		public void run() {
			while (true) {
				fetchUrls();
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					log.warning(e.getMessage());
				}
			}

		}
	}

	private static void fetchUrls() {
		try {
			Connection conn = ConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet res = stmt
					.executeQuery("select url from urls where stat=0 limit 1");

			String url = "http://www.36kr.com/";
			if (res.next()) {
				url = res.getString(1);
			}

			stmt.execute("update urls set stat=1 where url='" + url + "'");
			ConnectionFactory.close(stmt);
			ConnectionFactory.close(conn);
			Document doc = null;

			doc = Jsoup.connect(url).get();
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
						Connection conn2 = ConnectionFactory.getConnection();
						Statement stmt2 = conn2.createStatement();
						stmt2.execute("insert into urls(url,stat) values('"
								+ href + "',0)");
						ConnectionFactory.close(stmt2);
						ConnectionFactory.close(conn2);

					} catch (SQLException e) {
						if (e.getErrorCode() != 23505) {
							log.warning(e.getMessage());
						}

					}

				}
				if (m.matches()) {
					try {
						Connection conn3 = ConnectionFactory.getConnection();
						Statement stmt3 = conn3.createStatement();
						stmt3.execute("insert into article(url,stat) values('"
								+ href + "',0)");
						ConnectionFactory.close(stmt3);
						ConnectionFactory.close(conn3);

					} catch (SQLException e) {
						if (e.getErrorCode() != 23505) {
							log.warning(e.getMessage());
						}

					}

				}
			}
		} catch (Exception e) {
			log.warning(e.getMessage());
		}

		// fetchUrls(newurl);
	}

	static class Crawler extends Thread {

		@Override
		public void run() {
			while (true) {
				fetchData();
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					log.warning(e.getMessage());
				}
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
			log.warning(e.getMessage());
		} catch (SQLException e) {
			if (e.getErrorCode() != 23505) {
				log.warning(e.getMessage());
			}
			if (e.getErrorCode() == 2000) {

				try {
					Thread.sleep(50000);
				} catch (InterruptedException e1) {
					log.warning(e1.getMessage());
				}
			}

		} 
		catch (Exception e) {
			log.warning(e.getMessage());
		}
	}
}
