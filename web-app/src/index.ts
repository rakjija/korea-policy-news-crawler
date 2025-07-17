import path from "node:path";
import { Client } from "@elastic/elasticsearch";
import express from "express";

const app = express();
const port = 3000;

const ELASTICSEARCH_HOST =
	process.env.ELASTICSEARCH_HOST || "http://elasticsearch:9200";
const ELASTICSEARCH_INDEX =
	process.env.ELASTICSEARCH_INDEX || "korea-policy-news-*";

const esClient = new Client({ node: ELASTICSEARCH_HOST });

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

// 기본 라우트 (홈 페이지)
app.get("/", (_, res) => {
	res.render("index", { title: "뉴스 검색" });
});

// 뉴스 검색 API 엔드포인트
app.get("/api/news", async (req, res) => {
	try {
		const { q, page, size, start_date, end_date } = req.query;
		const pageNum = parseInt(page as string) || 1;
		const pageSize = parseInt(size as string) || 10;
		const from = (pageNum - 1) * pageSize;

		const body: any = {
			size: pageSize,
			from: from,
			query: {
				bool: {
					must: [],
					filter: [],
				},
			},
			sort: [{ published_at: { order: "desc" } }], // 최신 뉴스부터 정렬
		};

		// 검색어 (q) 처리
		if (q) {
			body.query.bool.must.push({
				multi_match: {
					query: q,
					fields: ["title", "contents", "subtitles", "publisher", "tags"],
				},
			});
		}

		// 날짜 범위 필터링
		if (start_date) {
			body.query.bool.filter.push({
				range: {
					published_at: { gte: start_date },
				},
			});
		}
		if (end_date) {
			body.query.bool.filter.push({
				range: {
					published_at: { lte: end_date },
				},
			});
		}

		const searchResult = await esClient.search({
			index: ELASTICSEARCH_INDEX,
			body: body,
		});

		const hits = searchResult.hits.hits.map((hit: any) => ({
			id: hit._id,
			...hit._source,
		}));
		const total = searchResult.hits.total
			? (searchResult.hits.total as any).value
			: 0;

		res.json({
			total: total,
			page: pageNum,
			size: pageSize,
			data: hits,
		});
	} catch (error: any) {
		console.error("Elasticsearch search error:", error);
		res
			.status(500)
			.json({ error: "Failed to fetch news data", details: error.message });
	}
});

app.listen(port, () => {
	console.log(`Server is running on http://localhost:${port}`);
});
