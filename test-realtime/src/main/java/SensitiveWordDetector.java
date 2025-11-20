import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SensitiveWordDetector {

    private static final String BASE_PATH = "D:\\workspace\\stream-bda-tzx\\stream-core\\src\\main\\resources\\";
    private static final Map<String, String> SENSITIVE_WORDS = new HashMap<String, String>();
    private static final Map<String, Integer> BAN_DAYS = new HashMap<String, Integer>();

    static {
        // 初始化封禁配置
        BAN_DAYS.put("P0", 365);
        BAN_DAYS.put("P1", 60);
        BAN_DAYS.put("P2", 0);
        BAN_DAYS.put("SUSPECTED", 0); // 疑似敏感词不封禁

        loadWordLibrary();

        // 运行测试
        testDetection();
    }

    /**
     * 获取敏感词映射表（只读）
     */
    public static Map<String, String> getSensitiveWords() {
        return new HashMap<>(SENSITIVE_WORDS);
    }

    /**
     * 获取敏感词级别
     */
    public static String getWordLevel(String word) {
        return SENSITIVE_WORDS.get(word);
    }

    /**
     * 加载词库
     */
    private static void loadWordLibrary() {
        try {
            // 加载标准敏感词库
            loadWordsFromFile("p0_words.txt", "P0");
            loadWordsFromFile("p1_words.txt", "P1");
            loadWordsFromFile("p2_words.txt", "P2");

            System.out.println("🎯 敏感词库加载完成 - P0:" + getWordCount("P0") +
                    ", P1:" + getWordCount("P1") +
                    ", P2:" + getWordCount("P2"));

            // 打印各级别关键词用于调试
            System.out.println("🔴 P0关键词示例: " + getSampleWords("P0", 5));
            System.out.println("🟡 P1关键词示例: " + getSampleWords("P1", 5));
            System.out.println("🟢 P2关键词示例: " + getSampleWords("P2", 10));

        } catch (Exception e) {
            System.err.println("❌ 加载敏感词库失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static long getWordCount(String level) {
        long count = 0;
        for (String value : SENSITIVE_WORDS.values()) {
            if (level.equals(value)) {
                count++;
            }
        }
        return count;
    }

    private static List<String> getSampleWords(String level, int max) {
        List<String> samples = new ArrayList<String>();
        for (Map.Entry<String, String> entry : SENSITIVE_WORDS.entrySet()) {
            if (level.equals(entry.getValue()) && samples.size() < max) {
                samples.add(entry.getKey());
            }
        }
        return samples;
    }

    /**
     * 从文件加载词库 - 增强调试版
     */
    private static void loadWordsFromFile(String fileName, String level) {
        try {
            String filePath = BASE_PATH + fileName;
            System.out.println("🚀 正在加载词库文件: " + filePath);

            if (!Files.exists(Paths.get(filePath))) {
                System.err.println("❌ 词库文件不存在: " + filePath);
                return;
            }

            List<String> lines = Files.readAllLines(Paths.get(filePath));
            int count = 0;
            int skipped = 0;

            System.out.println("📄 文件总行数: " + lines.size());

            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i).trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith("//")) {
                    skipped++;
                    continue;
                }

                // 打印前10行内容用于调试
                if (i < 10) {
                    System.out.println("📝 第" + (i+1) + "行: " + line);
                }

                // 支持逗号分隔的多个词汇
                if (line.contains(",")) {
                    String[] words = line.split(",");
                    for (String word : words) {
                        String trimmedWord = word.trim();
                        if (!trimmedWord.isEmpty() && trimmedWord.length() >= 1) {
                            SENSITIVE_WORDS.put(trimmedWord, level);
                            count++;
                            // 打印前5个加载的词汇
                            if (count <= 5) {
                                System.out.println("✅ 加载词汇: " + trimmedWord + " -> " + level);
                            }
                        }
                    }
                } else {
                    // 单词汇
                    if (line.length() >= 1) {
                        SENSITIVE_WORDS.put(line, level);
                        count++;
                        // 打印前5个加载的词汇
                        if (count <= 5) {
                            System.out.println("✅ 加载词汇: " + line + " -> " + level);
                        }
                    }
                }
            }

            System.out.println("🎯 加载 " + level + " 词库 [" + fileName + "]: " + count + " 个词" +
                    (skipped > 0 ? ", 跳过 " + skipped + " 行注释/空行" : ""));

        } catch (Exception e) {
            System.err.println("❌ 加载词库文件失败 " + fileName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 使用IK分词进行文本分词
     */
    private static List<String> segmentText(String text) {
        List<String> segments = new ArrayList<String>();
        if (text == null || text.trim().isEmpty()) {
            return segments;
        }

        try {
            StringReader reader = new StringReader(text);
            IKSegmenter segmenter = new IKSegmenter(reader, true); // 智能分词

            Lexeme lexeme;
            while ((lexeme = segmenter.next()) != null) {
                String word = lexeme.getLexemeText();
                if (word != null && word.length() >= 1) { // 不过滤单字
                    segments.add(word);
                }
            }
        } catch (Exception e) {
            System.err.println("❌ IK分词异常: " + e.getMessage());
        }

        return segments;
    }

    /**
     * 检测敏感词 - 超级调试版
     */
    public static SensitiveResult detect(String text) {
        if (text == null || text.trim().isEmpty()) {
            return new SensitiveResult(false, "CLEAN", "", new ArrayList<String>());
        }

        List<String> foundWords = new ArrayList<String>();
        String maxLevel = "CLEAN";
        String firstTriggeredWord = "";

        System.out.println("🔍 开始检测文本: " + text);
        System.out.println("📊 当前敏感词库总数: " + SENSITIVE_WORDS.size());

        // 方法1: 直接在整个文本中匹配敏感词
        int directMatchCount = 0;
        for (Map.Entry<String, String> entry : SENSITIVE_WORDS.entrySet()) {
            String word = entry.getKey();
            String level = entry.getValue();

            if (text.contains(word)) {
                System.out.println("✅ 直接匹配到敏感词: " + word + " -> " + level);
                directMatchCount++;

                if (!foundWords.contains(word)) {
                    foundWords.add(word);
                }

                if (firstTriggeredWord.isEmpty()) {
                    firstTriggeredWord = word;
                }

                if (getLevelWeight(level) > getLevelWeight(maxLevel)) {
                    maxLevel = level;
                }
            }
        }

        System.out.println("📈 直接匹配结果: " + directMatchCount + " 个匹配");

        // 方法2: 使用IK分词进行细粒度匹配
        List<String> segments = segmentText(text);
        System.out.println("🔤 分词结果: " + segments);

        int segmentMatchCount = 0;
        for (String segment : segments) {
            String wordLevel = SENSITIVE_WORDS.get(segment);
            if (wordLevel != null) {
                System.out.println("✅ 分词匹配到敏感词: " + segment + " -> " + wordLevel);
                segmentMatchCount++;

                if (!foundWords.contains(segment)) {
                    foundWords.add(segment);
                }

                if (firstTriggeredWord.isEmpty()) {
                    firstTriggeredWord = segment;
                }

                if (getLevelWeight(wordLevel) > getLevelWeight(maxLevel)) {
                    maxLevel = wordLevel;
                }
            }
        }

        System.out.println("📈 分词匹配结果: " + segmentMatchCount + " 个匹配");

        // 按优先级重新排序找到的敏感词
        if (!foundWords.isEmpty()) {
            Collections.sort(foundWords, new Comparator<String>() {
                @Override
                public int compare(String word1, String word2) {
                    String level1 = SENSITIVE_WORDS.get(word1);
                    String level2 = SENSITIVE_WORDS.get(word2);
                    return Integer.compare(getLevelWeight(level2), getLevelWeight(level1));
                }
            });

            firstTriggeredWord = foundWords.get(0);
        }

        boolean isSensitive = !foundWords.isEmpty() && !"SUSPECTED".equals(maxLevel);

        System.out.println("📊 最终检测结果: 敏感=" + isSensitive +
                ", 级别=" + maxLevel +
                ", 触发关键词=" + firstTriggeredWord +
                ", 所有检测到的词=" + foundWords +
                ", 封禁天数=" + getBanDays(maxLevel));

        return new SensitiveResult(isSensitive, maxLevel, firstTriggeredWord, foundWords);
    }

    /**
     * 获取级别权重
     */
    private static int getLevelWeight(String level) {
        if ("P0".equals(level)) {
            return 4;
        } else if ("P1".equals(level)) {
            return 3;
        } else if ("P2".equals(level)) {
            return 2;
        } else if ("SUSPECTED".equals(level)) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 获取封禁天数
     */
    public static int getBanDays(String level) {
        Integer days = BAN_DAYS.get(level);
        return days != null ? days : 0;
    }

    /**
     * 测试敏感词检测
     */
    public static void testDetection() {
        System.out.println("\n🧪 开始敏感词检测测试...");

        // 测试用例
        String[] testTexts = {
                "性价比很高，推荐购买！",
                "这件衣服质量很好",
                "测试共产党",
                "手机电脑电视",
                "快递发货很快",
                "推荐购买手机"
        };

        for (String text : testTexts) {
            System.out.println("\n=== 测试文本: " + text + " ===");
            SensitiveResult result = detect(text);
            System.out.println("结果: " + result.foundWords);
        }

        System.out.println("\n🧪 敏感词检测测试完成");
    }

    /**
     * 敏感检测结果
     */
    public static class SensitiveResult {
        public boolean isSensitive;
        public String level;
        public String triggeredKeyword;
        public List<String> foundWords;

        public SensitiveResult(boolean isSensitive, String level, String triggeredKeyword, List<String> foundWords) {
            this.isSensitive = isSensitive;
            this.level = level;
            this.triggeredKeyword = triggeredKeyword;
            this.foundWords = foundWords;
        }

        public int getBanDays() {
            return SensitiveWordDetector.getBanDays(level);
        }
    }
}