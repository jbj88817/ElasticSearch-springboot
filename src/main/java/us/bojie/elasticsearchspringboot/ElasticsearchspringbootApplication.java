package us.bojie.elasticsearchspringboot;


import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
import java.util.Date;

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
}
