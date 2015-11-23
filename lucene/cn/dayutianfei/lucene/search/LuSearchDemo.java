package cn.dayutianfei.lucene.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Lucene-5.2.1 Demo示例
 * @author egret
 *
 */
public class LuSearchDemo {

    /**
     * the path for index file;
     */
    private static String IndicesFilePath = "/tmp/lucene/index/";
    /**
     * true to delete old index and rebuild false to keep all history indexes.
     */
    private static boolean rebuild = false;
    /**
     * new index being built
     */
    private IndexWriter writer;
    private static Analyzer analyzer = new StandardAnalyzer();

    public LuSearchDemo() throws Exception {
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        Directory dir = FSDirectory.open(Paths.get(IndicesFilePath));
        if (rebuild) {
            iwc.setOpenMode(OpenMode.CREATE);
        }
        else {
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        }
        writer = new IndexWriter(dir, iwc);
    }

    public void indexDirectory(String datapath) throws Exception {
        File dataDir = new File(datapath);
        File[] dataFiles = dataDir.listFiles();
        for (File file : dataFiles) {
            FileInputStream inStream = new FileInputStream(file);
            Document doc = new Document();
            Field fullFileName = new StringField("fullFileName", file.getName(), Field.Store.YES);
            doc.add(fullFileName);
            System.out.println(file.getName());
            doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(inStream, "UTF-8"))));
            writer.addDocument(doc);
            writer.commit();
        }

    }

    public void indexDirectory(Document doc) throws Exception {
        writer.addDocument(doc);
        writer.commit();
    }

    public void commit() throws Exception {
        System.out.println("writer is to commit");
        writer.commit();
    }

    public void close() throws Exception {
        writer.commit();
        System.out.println("writer is to close");
        writer.close();
    }

    public void merge(int maxSegmentNumber) throws Exception {
        // MergePolicy.OneMerge mergeInfo = writer.getNextMerge();
        // System.out.println("merge info : " +
        // mergeInfo.getMergeInfo().toString());
        // writer.merge(mergeInfo);
        writer.forceMerge(maxSegmentNumber);
    }

    /* 搜索 */
    public void search() throws Exception {
        String field = "fullFileName"; // 搜索列
        String queryStr = "*:* and *:*"; // 搜索的字符串
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(IndicesFilePath)));
        IndexSearcher searcher = new IndexSearcher(reader);
        QueryParser parser = new QueryParser(field, analyzer);
        Query query = parser.parse(queryStr);
        System.out.println("Searching for: " + query.getClass() + query.toString(field));
        TopDocs results = searcher.search(query, 50);
        ScoreDoc[] hits = results.scoreDocs;
        int numTotalHits = results.totalHits;
        System.out.println(numTotalHits + " total matching in documents");
        for (ScoreDoc sd : hits) {
            Document doc = searcher.doc(sd.doc);
            System.out.println(doc.getFields().toString());
            System.out.println(doc.get("fullFileName"));
            System.out.println(sd.score);
        }
    }

    public static void main(String[] args) throws Exception {
        LuSearchDemo dd = new LuSearchDemo();
        // dd.indexDirectory("/temp/lucene/data");
        // dd.merge(2);
        // dd.close();
        dd.search();
    }
}
