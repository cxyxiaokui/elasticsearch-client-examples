package cn.zhuoqianmingyue.es.index;

/**
 * @Author： jkli
 * @Date： 2021/9/12 5:26 下午
 * @Description：
 **/
public class Demo {
    public static void main(String[] args) {
        String s = "{\n" +
                "  \"properties\": {\n" +
                "    \"description\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\",\n" +
                "      \"search_analyzer\": \"ik_smart\"\n" +
                "    },\n" +
                "    \"name\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\",\n" +
                "      \"search_analyzer\": \"ik_smart\"\n" +
                "    },\n" +
                "    \"pic\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"index\": false\n" +
                "    },\n" +
                "    \"price\": {\n" +
                "      \"type\": \"double\"\n" +
                "    },\n" +
                "    \"studymodel\": {\n" +
                "      \"type\": \"keyword\"\n" +
                "    },\n" +
                "    \"teachers\" : {\n" +
                "      \"type\" : \"text\",\n" +
                "      \"fields\" : {\n" +
                "        \"keyword\" : {\n" +
                "          \"type\" : \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"create_date\": {\n" +
                "      \"type\": \"date\",\n" +
                "      \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||yyyy-MM-dd'T'HH:mm:ss+0800\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        System.out.println(s);
    }
}
