import com.xhive.XhiveDriverFactory;
import com.xhive.core.interfaces.XhiveConsistencyCheckerIf;
import com.xhive.core.interfaces.XhiveDriverIf;
import com.xhive.core.interfaces.XhiveSessionIf;
import com.xhive.dom.interfaces.XhiveDocumentIf;
import com.xhive.dom.interfaces.XhiveElementIf;
import com.xhive.dom.interfaces.XhiveLibraryIf;
import com.xhive.query.interfaces.XhivePreparedQueryIf;
import com.xhive.query.interfaces.XhiveXQueryCompilerIf;
import com.xhive.query.interfaces.XhiveXQueryQueryIf;
import com.xhive.query.interfaces.XhiveXQueryResultIf;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class QueryLogParser {

   private static final Integer DEFAULT_NUM_TO_PRINT = 20;

   private static final Integer DEFAULT_CUTOFF_MS = 1000;

   private static final String LOG_LINE_START = "2014";

   private static final String QUERY_NAME_LINE_START = "Query - QueryString:";

   private static final Integer NUM_EXTRA_LINES = 7;

   private static final String FILE_PREFIX = "inv-svc-perf";

   private static final String GZ_SUFFIX = ".gz";

   private static final String CUTOFF_CODE = "c";

   private static final String CUTOFF_CODE_LONG = "cutoff";

   private static final String XDB_CODE = "x";

   private static final String XDB_CODE_LONG = "xdbPassword";

   private static final String XDB_BOOTSTRAP = "b";

   private static final String XDB_BOOTSTRAP_LONG = "xdbBootstrapFolder";

   private static final String NUM_TO_PRINT_CODE = "n";

   private static final String NUM_TO_PRINT_CODE_LONG = "numToPrint";

   private static final String PRINT_QUERY_PLAN = "p";

   private static final String PRINT_QUERY_PLAN_LONG = "printQueryPlan";

   private static final String PRINT_QUERY = "q";

   private static final String PRINT_QUERY_LONG = "printQuery";

   private static final String PRINT_JSON = "j";

   private static final String PRINT_JSON_LONG = "printAsJson";

   private static final String DB_NAME = "default";

   private static final JsonFactory _jsonFactory = new JsonFactory();

   private static final Pattern START_PATTERN =
         Pattern.compile(".*?query perf threshold warning \\(success = true\\).*");

   private static final Pattern END_PATTERN =
         Pattern.compile("^return <query:result xmlns.*");

   public static class QueryInfo implements Comparable<QueryInfo> {
      private static final String READABLE_OUTPUT =
            "-------\n" +
                  "Name: %s\n" +
                  "Occurrences: %d\n" +
                  "Median Time: %d\n" +
                  "Max Time: %d\n" +
                  "Min Time: %d\n";

      private static final String PAGES_READ_OUTPUT = "Sample Cache Pages Read: %d\n";

      private static final String CACHE_CREATED_OUTPUT = "Sample Cache Pages created: %d\n";

      private static final String QUERY_OUTPUT = "Sample Query:\n%s";

      private static final String QUERY_PLAN = "Query Plan:\n%s";

      private String _name;
      private String _query;
      private Double _medianTime;
      private Double _maxTime;
      private Double _minTime;
      private Integer _occurrences;
      private int _pagesRead = -1;
      private int _timeTaken = -1;
      private int _cacheCreated = -1;
      private XhiveDocumentIf _queryPlan;

      public QueryInfo(String name, Double medianTime, Double maxTime,
            Double minTime, Integer occurrences) {
         _name = name;
         _medianTime = medianTime;
         _maxTime = maxTime;
         _minTime = minTime;
         _occurrences = occurrences;
      }

      public QueryInfo(String name, Double medianTime, Double maxTime,
            Double minTime, Integer occurrences, String queryStr) {
         _name = name;
         _medianTime = medianTime;
         _maxTime = maxTime;
         _minTime = minTime;
         _occurrences = occurrences;
         _query = queryStr;
      }

      public String getName() {
         return _name;
      }

      public Double getMedianTime() {
         return _medianTime;
      }

      public Double getMaxTime() {
         return _maxTime;
      }

      public Double getMinTime() {
         return _minTime;
      }

      public String getQuery() {
         return _query;
      }

      public void setQueryPlan(XhiveDocumentIf queryPlan) {
         XhiveElementIf element = queryPlan.getDocumentElement().getFirstElementChild();
         _pagesRead = Integer.parseInt(element.getAttribute("pagesRead"));
         _timeTaken = Integer.parseInt(element.getAttribute("accumulatedTime"));
         _queryPlan = queryPlan;
      }

      @Override
      public String toString() {
         return String.format(READABLE_OUTPUT, _name,
               _occurrences, _medianTime.intValue(),
               _maxTime.intValue(), _minTime.intValue());
      }

      @Override
      public int compareTo(QueryInfo o) {
         return -1 * _medianTime.compareTo(o._medianTime);
      }

      @Override
      public boolean equals(Object obj) {
         return (obj instanceof QueryInfo) && ((QueryInfo)obj)._name.equals(_name);
      }

      @Override
      public int hashCode() {
         return _name.hashCode();
      }

      public Integer getOccurrences() {
         return _occurrences;
      }

      public String getString(boolean printQuery, boolean printQueryPlan) {
         StringBuilder strBuilder = new StringBuilder();
         strBuilder.append(String.format(READABLE_OUTPUT, _name,
               _occurrences, _medianTime.intValue(),
               _maxTime.intValue(), _minTime.intValue()));
         if (_pagesRead >= 0) {
            strBuilder.append(String.format(PAGES_READ_OUTPUT, _pagesRead));
         }
         if (_timeTaken >= 0) {
            //strBuilder.append(String.format(TIME_TAKEN_OUTPUT, _timeTaken));
         }
         if (_cacheCreated >= 0) {
            strBuilder.append(String.format(CACHE_CREATED_OUTPUT, _cacheCreated));
         }

         if (printQuery) {
            strBuilder.append(String.format(QUERY_OUTPUT, _query));
         }

         if (printQueryPlan) {
            strBuilder.append(String.format(QUERY_PLAN, _queryPlan.toXml()));
         }
         return strBuilder.toString();
      }

      public String getJsonString(boolean printQuery, boolean printQueryPlan)
            throws IOException {
         StringWriter writer = new StringWriter();

         JsonGenerator jsonGen = _jsonFactory.createJsonGenerator(writer);
         jsonGen.writeStartObject();
         jsonGen.writeStringField("queryName", _name);
         jsonGen.writeNumberField("occurrences", _occurrences);
         jsonGen.writeNumberField("medianTime", _medianTime);
         jsonGen.writeNumberField("maxTime", _maxTime);
         jsonGen.writeNumberField("minTime", _minTime);
         if (printQuery) {
            jsonGen.writeStringField("queryString", _query);
         }
         if (_pagesRead >= 0) {
            jsonGen.writeNumberField("samplePagesRead", _pagesRead);
         }
         if (_timeTaken >= 0) {
            //jsonGen.writeNumberField("sampleTimeTaken", _timeTaken);
         }
         if (_cacheCreated >= 0) {
            jsonGen.writeNumberField("sampleCacheCreated", _cacheCreated);
         }
         if (printQueryPlan && _queryPlan != null) {
            jsonGen.writeStringField("queryPlan", _queryPlan.toXml());
         }
         jsonGen.writeEndObject();
         jsonGen.close();
         return writer.toString();
      }

      public void setCacheCreated(int cacheCreated) {
         this._cacheCreated = cacheCreated;
      }

      public int getCacheCreated() {
         return _cacheCreated;
      }
   }

   public static class ISPerfFilter implements IOFileFilter {
      @Override
      public boolean accept(File file) {
         return file.getName().startsWith(FILE_PREFIX);
      }

      @Override
      public boolean accept(File file, String s) {
         return s.startsWith(FILE_PREFIX);
      }
   }

   public static void main(String[] args) throws IOException {
      Options options = new Options();
      options.addOption(NUM_TO_PRINT_CODE, NUM_TO_PRINT_CODE_LONG, true,
            "number of top queries to print.");
      options.addOption(CUTOFF_CODE, CUTOFF_CODE_LONG, true,
            "cutoff (in ms) to use.");
      options.addOption(XDB_CODE, XDB_CODE_LONG, true,
            "xdb password. Won't run queries if not present.");
      options.addOption(XDB_BOOTSTRAP, XDB_BOOTSTRAP_LONG, true,
            "xdb bootstrap file. Won't run queries if not present.");
      options.addOption(PRINT_QUERY, PRINT_QUERY_LONG, false,
            "output the query string.");
      options.addOption(PRINT_QUERY_PLAN, PRINT_QUERY_PLAN_LONG, false,
            "output the query plan returned by XDB.");
      options.addOption(PRINT_JSON, PRINT_JSON_LONG, false,
            "output in JSON instead of human-readable-format.");


      CommandLineParser parser = new BasicParser();
      CommandLine commands = null;
      try {
         commands = parser.parse(options, args);
      } catch (ParseException e) {
         System.out.println("Error parsing args: " + e.getMessage());
         return;
      }

      String[] restOfArgs = commands.getArgs();
      if (restOfArgs.length != 1) {
         HelpFormatter helpMsg = new HelpFormatter();
         helpMsg.printHelp("QueryLogParser path-to-log-folder [options]",
               options);
         return;
      }
      String logFolder = restOfArgs[0];
      boolean printQuery = commands.hasOption(PRINT_QUERY) ? true : false;
      boolean printQueryPlan =
            commands.hasOption(PRINT_QUERY_PLAN) ? true : false;
      boolean printJson = commands.hasOption(PRINT_JSON) ? true : false;
      int numToPrint = commands.hasOption(NUM_TO_PRINT_CODE) ?
            Integer.parseInt(commands.getOptionValue(NUM_TO_PRINT_CODE)) :
            DEFAULT_NUM_TO_PRINT;
      int cutoff = commands.hasOption(CUTOFF_CODE) ?
            Integer.parseInt(commands.getOptionValue(CUTOFF_CODE)) :
            DEFAULT_CUTOFF_MS;
      String xdbPassword = commands.getOptionValue(XDB_CODE);
      String xdbBootstrapPath = commands.getOptionValue(XDB_BOOTSTRAP);


      Map<String, List<Integer>> queryTimes = new HashMap<>();

      File logFldr = new File(logFolder);
      for (File file : FileUtils.listFiles(logFldr, new ISPerfFilter(), null)) {
         if (file.getName().endsWith(GZ_SUFFIX)) {
            try (BufferedReader reader = new BufferedReader(
                  new InputStreamReader(
                        new GZIPInputStream(new FileInputStream(file)))
            )) {
               processReader(reader, queryTimes);
            }
         } else {
            try (BufferedReader reader = new BufferedReader(
                  new InputStreamReader(new FileInputStream(file)))) {
               processReader(reader, queryTimes);
            }
         }
      }

      // Post-processing to get top list of queries
      SortedSet<QueryInfo> allMedianTimes = new TreeSet<>();
      for (Map.Entry<String, List<Integer>> entry : queryTimes.entrySet()) {
         DescriptiveStatistics stats = new DescriptiveStatistics();
         for (Integer time : entry.getValue()) {
            stats.addValue(time);
         }
         if (stats.getPercentile(50) > cutoff) {
            allMedianTimes.add(new QueryInfo(entry.getKey(),
                  stats.getPercentile(50),
                  stats.getMax(),
                  stats.getMin(),
                  entry.getValue().size()));
         }
      }

      SortedSet<QueryInfo> topMedianTimes = new TreeSet<>();
      for (QueryInfo info : allMedianTimes) {
         if (numToPrint-- > 0) {
            topMedianTimes.add(info);
         }
      }

      // Look through the files again to get the query strings
      // for the top queries. Only get a single string.
      SortedSet<QueryInfo> processedMedianTimes = new TreeSet<>();
      for (File file : FileUtils.listFiles(logFldr, new ISPerfFilter(), null)) {
         if (file.getName().endsWith(GZ_SUFFIX)) {
            try (BufferedReader reader = new BufferedReader(
                  new InputStreamReader(
                        new GZIPInputStream(new FileInputStream(file)))
            )) {
               processedMedianTimes.addAll(
                     findQueries(reader, topMedianTimes));
            }
         } else {
            try (BufferedReader reader = new BufferedReader(
                  new InputStreamReader(new FileInputStream(file)))) {
               processedMedianTimes.addAll(
                     findQueries(reader, topMedianTimes));
            }
         }
      }

      // Connect to local XDB instance and run top queries against XDB with
      // profiling enabled if possible.
      if (xdbPassword != null && xdbBootstrapPath != null) {
         XhiveDriverIf xdbDriver =
               XhiveDriverFactory.getDriver(xdbBootstrapPath);
         xdbDriver.init();
         XhiveSessionIf session = xdbDriver.createSession();
         try {
            session.connect(XhiveSessionIf.ADMINISTRATOR_NAME, xdbPassword,
                  DB_NAME);
            session.setReadOnlyMode(true);
            session.begin();
            session.commit();
            for (QueryInfo query : processedMedianTimes) {
               int startCacheCreated =
                     xdbDriver.getStatistics().get("cache pages created")
                           .intValue();
               session.begin();
               XhiveLibraryIf rootLibrary = session.getDatabase().getRoot();
               XhiveXQueryCompilerIf xQueryCompiler =
                     session.getXQueryCompiler();
               xQueryCompiler
                     .setOption(XhiveXQueryQueryIf.XHIVE_PROFILE, "true");
               XhivePreparedQueryIf prepQuery =
                     xQueryCompiler.prepareQuery(query.getQuery());

               XhiveXQueryQueryIf xQuery =
                     prepQuery.createQuery(rootLibrary);
               xQuery.setFunction("serviceUuid", new StoreUuidExtension());
               xQuery.setFunction("product", new ProductExtension(session));
               xQuery.setFunction("providerUuid",
                     new ProviderUuidExtension());
               xQuery.setFunction("hasPrivilege", new AuthExtension());
               XhiveXQueryResultIf result = xQuery.execute();
               while (result.hasNext()) {
                  result.next();
               }
               XhiveDocumentIf queryPlanDoc = result.getQueryPlan();
               XhiveElementIf element =
                     queryPlanDoc.getDocumentElement().getFirstElementChild();
               query.setQueryPlan(queryPlanDoc);
               session.commit();
               query.setCacheCreated(
                     xdbDriver.getStatistics().get("cache pages created")
                           .intValue() - startCacheCreated);
               System.out.println(xdbDriver.getStatistics());
            }
            session.begin();
            XhiveConsistencyCheckerIf checker = session.getDatabase().getConsistencyChecker();
            StringWriter writer = new StringWriter();
            PrintWriter printer = new PrintWriter(writer);
            checker.setPrintWriter(printer);
            checker.checkDatabaseConsistency();
            printer.flush();
            System.out.println(writer.toString());
            session.commit();
         } finally {
            // disconnect and remove the session
            if (session.isOpen()) {
               session.rollback();
            }
            if (session.isConnected()) {
               session.disconnect();
            }
            xdbDriver.close();
         }
      }

      // Get the text content of top queries
      for (QueryInfo info : processedMedianTimes) {
         if (printJson) {
            System.out.println(info.getJsonString(printQuery, printQueryPlan));
         } else {
            System.out.println(info.getString(printQuery, printQueryPlan));
         }
      }
   }

   private static List<QueryInfo> findQueries(BufferedReader reader,
         SortedSet<QueryInfo> unprocessedMedianTimes) throws IOException {
      if (unprocessedMedianTimes.isEmpty()) {
         return new ArrayList<>();
      }
      String nextStr = reader.readLine();
      boolean startRecording = false;
      List<QueryInfo> foundInfos = new ArrayList<>();
      QueryInfo activeInfo = null;
      StringBuilder queryStrBuilder = null;
      while (nextStr != null) {
         if (!startRecording && !nextStr.startsWith(QUERY_NAME_LINE_START)) {
            // Ignore
         } else if (!startRecording) {
            for (QueryInfo info : unprocessedMedianTimes) {
               if (nextStr.contains(info.getName())) {
                  startRecording = true;
                  activeInfo = info;
                  unprocessedMedianTimes.remove(activeInfo);
                  queryStrBuilder = new StringBuilder();
                  break;
               }
            }
         } else if (END_PATTERN.matcher(nextStr).find()) {
            queryStrBuilder.append(nextStr.split(" Product: ")[0]).append('\n');
            startRecording = false;
            foundInfos.add(new QueryInfo(activeInfo.getName(),
                  activeInfo.getMedianTime(),
                  activeInfo.getMaxTime(),
                  activeInfo.getMinTime(),
                  activeInfo.getOccurrences(),
                  queryStrBuilder.toString()));
            queryStrBuilder = null;
            activeInfo = null;
         } else {
            // Must be a line in the query
            queryStrBuilder.append(nextStr).append('\n');
         }
         nextStr = reader.readLine();
      }
      return foundInfos;
   }

   private static void processReader(BufferedReader reader,
         Map<String, List<Integer>> queryTimes) throws IOException {
      String nextStr = reader.readLine();
      boolean startRecording = false;
      while (nextStr != null) {
         if (!nextStr.startsWith(LOG_LINE_START)) {
            nextStr = reader.readLine();
            continue;
         }
         if (START_PATTERN.matcher(nextStr).find()) {
            assert(!startRecording);
            startRecording = true;
            Integer queryTime =
                  new Integer(reader.readLine().split("ms")[0].split(": ")[1]);
            for (int i = 0; i < NUM_EXTRA_LINES; ++i) {
               // Ignore the other totals
               reader.readLine();
            }
            nextStr = reader.readLine();
            String[] nameArray = nextStr.split("Query name: ");
            if (nameArray.length > 1) {
               String queryName = nameArray[1].split(" :\\)")[0];
               List<Integer> timesList = queryTimes.get(queryName);
               if (timesList == null) {
                  timesList = new LinkedList<>();
                  timesList.add(queryTime);
                  queryTimes.put(queryName, timesList);
               } else {
                  timesList.add(queryTime);
               }
            } else {
               startRecording = false;
            }
         } else if (END_PATTERN.matcher(nextStr).find() && startRecording) {
            startRecording = false;
         }
         nextStr = reader.readLine();
      }
   }
}
