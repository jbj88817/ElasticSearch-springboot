package us.bojie.elasticsearchspringboot;


import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
public class ElasticsearchspringbootApplication {

    @Autowired
    private TransportClient mClient;

    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchspringbootApplication.class, args);
    }

    @GetMapping("/")
    public String index() {
        return "index";
    }

    // Getting
    @GetMapping("/get/book/novel")
    public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {
        if (id.isEmpty()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        GetResponse result = mClient.prepareGet("book", "novel", id)
                .get();

        if (!result.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        return new ResponseEntity(result.getSource(), HttpStatus.OK);
    }

    // Adding
    @PostMapping("add/book/novel")
    public ResponseEntity add(
            @RequestParam(name = "title") String title,
            @RequestParam(name = "author") String author,
            @RequestParam(name = "word_count") int wordCount,
            @RequestParam(name = "publish_date")
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                    Date publishDate) {
        try {
            XContentBuilder content = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("title", title)
                    .field("author", author)
                    .field("word_count", wordCount)
                    .field("publish_date", publishDate.getTime())
                    .endObject();
            IndexResponse result = mClient.prepareIndex("book", "novel")
                    .setSource(content)
                    .get();
            return new ResponseEntity(result.getId(), HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // Deleting
    @DeleteMapping("/delete/book/novel")
    public ResponseEntity delete(@RequestParam(name = "id", defaultValue = "") String id) {
        if (StringUtils.isEmpty(id)) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        DeleteResponse response = mClient.prepareDelete("book", "novel", id).get();
        return new ResponseEntity(response.getResult().toString(), HttpStatus.OK);
    }

    // Updating
    @PutMapping("update/book/novel")
    public ResponseEntity update(
            @RequestParam(name = "id") String id,
            @RequestParam(name = "title", required = false) String title,
            @RequestParam(name = "author", required = false) String author
    ) {
        UpdateRequest update = new UpdateRequest("book", "novel", id);

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject();
            if (title != null) {
                builder.field("title", title);
            }
            if (author != null) {
                builder.field("author", author);
            }
            builder.endObject();
            update.doc(builder);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        try {
            UpdateResponse result = mClient.update(update).get();
            return new ResponseEntity(result.getResult().toString(), HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("query/book/novel")
    public ResponseEntity query(
            @RequestParam(name = "author", required = false) String author,
            @RequestParam(name = "title", required = false) String title,
            @RequestParam(name = "gt_word_count", defaultValue = "0") int gtWordCount,
            @RequestParam(name = "lt_word_count", required = false) Integer ltWordCount
    ) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (author != null) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("author", author));
        }

        if (title != null) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("title", title));
        }
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("word_count")
                .from(gtWordCount);
        if (ltWordCount != null && ltWordCount > 0) {
            rangeQueryBuilder.to(ltWordCount);
        }
        boolQueryBuilder.filter(rangeQueryBuilder);

        SearchRequestBuilder searchRequestBuilder = mClient.prepareSearch("book")
                .setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder)
                .setFrom(0)
                .setSize(10);
        System.out.println(searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();
        List<Map<String, Object>> result = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits()) {
            result.add(hit.getSource());
        }

        return new ResponseEntity(result, HttpStatus.OK);
    }
}
