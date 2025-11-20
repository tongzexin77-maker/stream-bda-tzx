package utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * IP工具类 - 增强版（包含运营商识别和真实地区分布）
 */
public class IpUtils {

    // 运营商IP段映射
    private static final Map<String, String[]> OPERATOR_IP_RANGES = new HashMap<>();
    private static final Random random = new Random();

    // 省份IP段映射（模拟真实IP分布）
    private static final Map<String, String[]> PROVINCE_IP_RANGES = new HashMap<>();
    private static final Map<String, String[]> PROVINCE_CITIES = new HashMap<>();
    private static final Map<String, Integer> PROVINCE_WEIGHTS = new HashMap<>();

    static {
        // 初始化运营商IP段
        initOperatorIPRanges();

        // 初始化省份IP段
        initProvinceIPRanges();

        // 初始化省份城市映射
        initProvinceCities();

        // 初始化省份权重
        initProvinceWeights();
    }

    /**
     * 初始化运营商IP段
     */
    private static void initOperatorIPRanges() {
        // 中国电信IP段
        OPERATOR_IP_RANGES.put("电信", new String[]{
                "14.0.0.0-14.255.255.255", "27.0.0.0-27.255.255.255",
                "36.0.0.0-36.255.255.255", "49.0.0.0-49.255.255.255",
                "58.0.0.0-58.255.255.255", "60.0.0.0-60.255.255.255",
                "110.0.0.0-110.255.255.255", "111.0.0.0-111.255.255.255",
                "112.0.0.0-112.255.255.255", "113.0.0.0-113.255.255.255",
                "114.0.0.0-114.255.255.255", "115.0.0.0-115.255.255.255",
                "116.0.0.0-116.255.255.255", "117.0.0.0-117.255.255.255",
                "118.0.0.0-118.255.255.255", "119.0.0.0-119.255.255.255",
                "120.0.0.0-120.255.255.255", "121.0.0.0-121.255.255.255",
                "122.0.0.0-122.255.255.255", "123.0.0.0-123.255.255.255",
                "124.0.0.0-124.255.255.255", "125.0.0.0-125.255.255.255",
                "171.0.0.0-171.255.255.255", "175.0.0.0-175.255.255.255",
                "180.0.0.0-180.255.255.255", "183.0.0.0-183.255.255.255",
                "218.0.0.0-218.255.255.255", "219.0.0.0-219.255.255.255",
                "220.0.0.0-220.255.255.255", "221.0.0.0-221.255.255.255",
                "222.0.0.0-222.255.255.255"
        });

        // 中国联通IP段
        OPERATOR_IP_RANGES.put("联通", new String[]{
                "42.0.0.0-42.255.255.255", "43.0.0.0-43.255.255.255",
                "58.0.0.0-58.255.255.255", "59.0.0.0-59.255.255.255",
                "60.0.0.0-60.255.255.255", "61.0.0.0-61.255.255.255",
                "106.0.0.0-106.255.255.255", "110.0.0.0-110.255.255.255",
                "111.0.0.0-111.255.255.255", "112.0.0.0-112.255.255.255",
                "113.0.0.0-113.255.255.255", "114.0.0.0-114.255.255.255",
                "115.0.0.0-115.255.255.255", "116.0.0.0-116.255.255.255",
                "117.0.0.0-117.255.255.255", "118.0.0.0-118.255.255.255",
                "119.0.0.0-119.255.255.255", "120.0.0.0-120.255.255.255",
                "121.0.0.0-121.255.255.255", "122.0.0.0-122.255.255.255",
                "123.0.0.0-123.255.255.255", "124.0.0.0-124.255.255.255",
                "125.0.0.0-125.255.255.255", "171.0.0.0-171.255.255.255",
                "175.0.0.0-175.255.255.255", "176.0.0.0-176.255.255.255",
                "182.0.0.0-182.255.255.255", "183.0.0.0-183.255.255.255",
                "210.0.0.0-210.255.255.255", "211.0.0.0-211.255.255.255",
                "218.0.0.0-218.255.255.255", "219.0.0.0-219.255.255.255",
                "220.0.0.0-220.255.255.255", "221.0.0.0-221.255.255.255",
                "222.0.0.0-222.255.255.255"
        });

        // 中国移动IP段
        OPERATOR_IP_RANGES.put("移动", new String[]{
                "36.0.0.0-36.255.255.255", "39.0.0.0-39.255.255.255",
                "42.0.0.0-42.255.255.255", "49.0.0.0-49.255.255.255",
                "58.0.0.0-58.255.255.255", "59.0.0.0-59.255.255.255",
                "60.0.0.0-60.255.255.255", "61.0.0.0-61.255.255.255",
                "106.0.0.0-106.255.255.255", "111.0.0.0-111.255.255.255",
                "112.0.0.0-112.255.255.255", "113.0.0.0-113.255.255.255",
                "114.0.0.0-114.255.255.255", "115.0.0.0-115.255.255.255",
                "117.0.0.0-117.255.255.255", "118.0.0.0-118.255.255.255",
                "120.0.0.0-120.255.255.255", "121.0.0.0-121.255.255.255",
                "122.0.0.0-122.255.255.255", "123.0.0.0-123.255.255.255",
                "124.0.0.0-124.255.255.255", "125.0.0.0-125.255.255.255",
                "139.0.0.0-139.255.255.255", "171.0.0.0-171.255.255.255",
                "175.0.0.0-175.255.255.255", "180.0.0.0-180.255.255.255",
                "183.0.0.0-183.255.255.255", "202.0.0.0-202.255.255.255",
                "211.0.0.0-211.255.255.255", "218.0.0.0-218.255.255.255",
                "219.0.0.0-219.255.255.255", "220.0.0.0-220.255.255.255",
                "221.0.0.0-221.255.255.255", "222.0.0.0-222.255.255.255"
        });

        // 其他运营商
        OPERATOR_IP_RANGES.put("铁通", new String[]{
                "14.0.0.0-14.255.255.255", "42.0.0.0-42.255.255.255",
                "58.0.0.0-58.255.255.255", "59.0.0.0-59.255.255.255",
                "60.0.0.0-60.255.255.255", "61.0.0.0-61.255.255.255",
                "110.0.0.0-110.255.255.255", "111.0.0.0-111.255.255.255",
                "112.0.0.0-112.255.255.255", "113.0.0.0-113.255.255.255",
                "114.0.0.0-114.255.255.255", "115.0.0.0-115.255.255.255",
                "116.0.0.0-116.255.255.255", "117.0.0.0-117.255.255.255",
                "118.0.0.0-118.255.255.255", "119.0.0.0-119.255.255.255",
                "120.0.0.0-120.255.255.255", "121.0.0.0-121.255.255.255",
                "122.0.0.0-122.255.255.255", "123.0.0.0-123.255.255.255",
                "124.0.0.0-124.255.255.255", "125.0.0.0-125.255.255.255",
                "171.0.0.0-171.255.255.255", "175.0.0.0-175.255.255.255",
                "180.0.0.0-180.255.255.255", "182.0.0.0-182.255.255.255",
                "183.0.0.0-183.255.255.255", "202.0.0.0-202.255.255.255",
                "210.0.0.0-210.255.255.255", "211.0.0.0-211.255.255.255",
                "218.0.0.0-218.255.255.255", "219.0.0.0-219.255.255.255",
                "220.0.0.0-220.255.255.255", "221.0.0.0-221.255.255.255",
                "222.0.0.0-222.255.255.255"
        });
    }

    /**
     * 初始化省份IP段映射
     */
    private static void initProvinceIPRanges() {
        // 北京市IP段
        PROVINCE_IP_RANGES.put("北京市", new String[]{
                "1.0.1.0-1.0.3.255", "1.1.0.0-1.1.255.255",
                "14.0.0.0-14.255.255.255", "27.0.0.0-27.255.255.255",
                "36.0.0.0-36.255.255.255", "42.0.0.0-42.255.255.255"
        });

        // 天津市IP段
        PROVINCE_IP_RANGES.put("天津市", new String[]{
                "1.0.8.0-1.0.15.255", "1.1.0.0-1.1.255.255",
                "36.0.0.0-36.255.255.255", "42.0.0.0-42.255.255.255",
                "58.0.0.0-58.255.255.255", "60.0.0.0-60.255.255.255"
        });

        // 上海市IP段
        PROVINCE_IP_RANGES.put("上海市", new String[]{
                "1.0.32.0-1.0.63.255", "1.1.0.0-1.1.255.255",
                "58.0.0.0-58.255.255.255", "101.0.0.0-101.255.255.255",
                "112.0.0.0-112.255.255.255", "116.0.0.0-116.255.255.255"
        });

        // 重庆市IP段
        PROVINCE_IP_RANGES.put("重庆市", new String[]{
                "1.0.64.0-1.0.127.255", "1.1.0.0-1.1.255.255",
                "106.0.0.0-106.255.255.255", "111.0.0.0-111.255.255.255",
                "118.0.0.0-118.255.255.255", "123.0.0.0-123.255.255.255"
        });

        // 广东省IP段
        PROVINCE_IP_RANGES.put("广东省", new String[]{
                "1.0.128.0-1.0.255.255", "14.0.0.0-14.255.255.255",
                "58.0.0.0-58.255.255.255", "112.0.0.0-112.255.255.255",
                "113.0.0.0-113.255.255.255", "116.0.0.0-116.255.255.255",
                "117.0.0.0-117.255.255.255", "120.0.0.0-120.255.255.255"
        });

        // 江苏省IP段
        PROVINCE_IP_RANGES.put("江苏省", new String[]{
                "1.0.32.0-1.0.63.255", "58.0.0.0-58.255.255.255",
                "112.0.0.0-112.255.255.255", "114.0.0.0-114.255.255.255",
                "117.0.0.0-117.255.255.255", "121.0.0.0-121.255.255.255",
                "122.0.0.0-122.255.255.255", "123.0.0.0-123.255.255.255"
        });

        // 浙江省IP段
        PROVINCE_IP_RANGES.put("浙江省", new String[]{
                "1.0.64.0-1.0.127.255", "60.0.0.0-60.255.255.255",
                "115.0.0.0-115.255.255.255", "122.0.0.0-122.255.255.255",
                "124.0.0.0-124.255.255.255", "125.0.0.0-125.255.255.255"
        });

        // 山东省IP段
        PROVINCE_IP_RANGES.put("山东省", new String[]{
                "1.0.128.0-1.0.255.255", "58.0.0.0-58.255.255.255",
                "112.0.0.0-112.255.255.255", "119.0.0.0-119.255.255.255",
                "120.0.0.0-120.255.255.255", "121.0.0.0-121.255.255.255"
        });

        // 河南省IP段
        PROVINCE_IP_RANGES.put("河南省", new String[]{
                "1.0.32.0-1.0.63.255", "42.0.0.0-42.255.255.255",
                "58.0.0.0-58.255.255.255", "123.0.0.0-123.255.255.255",
                "124.0.0.0-124.255.255.255", "125.0.0.0-125.255.255.255"
        });

        // 四川省IP段
        PROVINCE_IP_RANGES.put("四川省", new String[]{
                "1.0.64.0-1.0.127.255", "42.0.0.0-42.255.255.255",
                "106.0.0.0-106.255.255.255", "110.0.0.0-110.255.255.255",
                "111.0.0.0-111.255.255.255", "118.0.0.0-118.255.255.255"
        });

        // 湖北省IP段
        PROVINCE_IP_RANGES.put("湖北省", new String[]{
                "1.0.128.0-1.0.255.255", "58.0.0.0-58.255.255.255",
                "112.0.0.0-112.255.255.255", "113.0.0.0-113.255.255.255",
                "114.0.0.0-114.255.255.255", "115.0.0.0-115.255.255.255"
        });
    }

    /**
     * 初始化省份权重
     */
    private static void initProvinceWeights() {
        PROVINCE_WEIGHTS.put("广东省", 15);
        PROVINCE_WEIGHTS.put("江苏省", 12);
        PROVINCE_WEIGHTS.put("浙江省", 10);
        PROVINCE_WEIGHTS.put("山东省", 9);
        PROVINCE_WEIGHTS.put("河南省", 8);
        PROVINCE_WEIGHTS.put("四川省", 7);
        PROVINCE_WEIGHTS.put("河北省", 6);
        PROVINCE_WEIGHTS.put("湖南省", 5);
        PROVINCE_WEIGHTS.put("湖北省", 5);
        PROVINCE_WEIGHTS.put("安徽省", 4);
        PROVINCE_WEIGHTS.put("福建省", 4);
        PROVINCE_WEIGHTS.put("北京市", 3);
        PROVINCE_WEIGHTS.put("上海市", 3);
        PROVINCE_WEIGHTS.put("陕西省", 3);
        PROVINCE_WEIGHTS.put("辽宁省", 2);
        PROVINCE_WEIGHTS.put("江西省", 2);
        PROVINCE_WEIGHTS.put("广西壮族自治区", 2);
        PROVINCE_WEIGHTS.put("云南省", 2);
        PROVINCE_WEIGHTS.put("黑龙江省", 1);
        PROVINCE_WEIGHTS.put("吉林省", 1);
        PROVINCE_WEIGHTS.put("山西省", 1);
        PROVINCE_WEIGHTS.put("贵州省", 1);
        PROVINCE_WEIGHTS.put("甘肃省", 1);
        PROVINCE_WEIGHTS.put("内蒙古自治区", 1);
        PROVINCE_WEIGHTS.put("新疆维吾尔自治区", 1);
        PROVINCE_WEIGHTS.put("天津市", 1);
        PROVINCE_WEIGHTS.put("海南省", 1);
        PROVINCE_WEIGHTS.put("宁夏回族自治区", 1);
        PROVINCE_WEIGHTS.put("青海省", 1);
        PROVINCE_WEIGHTS.put("西藏自治区", 1);
    }

    /**
     * 初始化省份城市映射
     */
    private static void initProvinceCities() {
        // 直辖市
        PROVINCE_CITIES.put("北京市", new String[]{"北京市"});
        PROVINCE_CITIES.put("天津市", new String[]{"天津市"});
        PROVINCE_CITIES.put("上海市", new String[]{"上海市"});
        PROVINCE_CITIES.put("重庆市", new String[]{"重庆市"});

        // 广东省
        PROVINCE_CITIES.put("广东省", new String[]{
                "广州市", "深圳市", "东莞市", "佛山市", "珠海市", "中山市", "惠州市", "江门市",
                "汕头市", "湛江市", "韶关市", "肇庆市", "茂名市", "梅州市", "汕尾市", "河源市"
        });

        // 江苏省
        PROVINCE_CITIES.put("江苏省", new String[]{
                "南京市", "苏州市", "无锡市", "常州市", "徐州市", "南通市", "扬州市", "镇江市",
                "盐城市", "泰州市", "连云港市", "淮安市", "宿迁市"
        });

        // 浙江省
        PROVINCE_CITIES.put("浙江省", new String[]{
                "杭州市", "宁波市", "温州市", "嘉兴市", "湖州市", "绍兴市", "金华市", "台州市",
                "衢州市", "丽水市", "舟山市"
        });

        // 山东省
        PROVINCE_CITIES.put("山东省", new String[]{
                "济南市", "青岛市", "淄博市", "枣庄市", "东营市", "烟台市", "潍坊市", "济宁市",
                "泰安市", "威海市", "日照市", "临沂市", "德州市", "聊城市", "滨州市", "菏泽市"
        });

        // 河南省
        PROVINCE_CITIES.put("河南省", new String[]{
                "郑州市", "开封市", "洛阳市", "平顶山市", "安阳市", "鹤壁市", "新乡市", "焦作市",
                "濮阳市", "许昌市", "漯河市", "三门峡市", "南阳市", "商丘市", "信阳市", "周口市", "驻马店市"
        });

        // 四川省
        PROVINCE_CITIES.put("四川省", new String[]{
                "成都市", "自贡市", "攀枝花市", "泸州市", "德阳市", "绵阳市", "广元市", "遂宁市",
                "内江市", "乐山市", "南充市", "眉山市", "宜宾市", "广安市", "达州市", "雅安市", "巴中市", "资阳市"
        });

        // 湖北省
        PROVINCE_CITIES.put("湖北省", new String[]{
                "武汉市", "黄石市", "十堰市", "宜昌市", "襄阳市", "鄂州市", "荆门市", "孝感市",
                "荆州市", "黄冈市", "咸宁市", "随州市", "恩施土家族苗族自治州"
        });

        // 其他省份...
        PROVINCE_CITIES.put("河北省", new String[]{"石家庄市", "唐山市", "秦皇岛市", "邯郸市", "邢台市", "保定市", "张家口市", "承德市"});
        PROVINCE_CITIES.put("湖南省", new String[]{"长沙市", "株洲市", "湘潭市", "衡阳市", "邵阳市", "岳阳市", "常德市", "张家界市"});
        PROVINCE_CITIES.put("安徽省", new String[]{"合肥市", "芜湖市", "蚌埠市", "淮南市", "马鞍山市", "淮北市", "铜陵市", "安庆市"});
        PROVINCE_CITIES.put("福建省", new String[]{"福州市", "厦门市", "莆田市", "三明市", "泉州市", "漳州市", "南平市", "龙岩市"});
        PROVINCE_CITIES.put("陕西省", new String[]{"西安市", "铜川市", "宝鸡市", "咸阳市", "渭南市", "延安市", "汉中市", "榆林市"});
        PROVINCE_CITIES.put("辽宁省", new String[]{"沈阳市", "大连市", "鞍山市", "抚顺市", "本溪市", "丹东市", "锦州市", "营口市"});
        PROVINCE_CITIES.put("江西省", new String[]{"南昌市", "景德镇市", "萍乡市", "九江市", "新余市", "鹰潭市", "赣州市", "吉安市"});
        PROVINCE_CITIES.put("广西壮族自治区", new String[]{"南宁市", "柳州市", "桂林市", "梧州市", "北海市", "防城港市", "钦州市", "贵港市"});
        PROVINCE_CITIES.put("云南省", new String[]{"昆明市", "曲靖市", "玉溪市", "保山市", "昭通市", "丽江市", "普洱市", "临沧市"});
        PROVINCE_CITIES.put("黑龙江省", new String[]{"哈尔滨市", "齐齐哈尔市", "鸡西市", "鹤岗市", "双鸭山市", "大庆市", "伊春市", "佳木斯市"});
        PROVINCE_CITIES.put("吉林省", new String[]{"长春市", "吉林市", "四平市", "辽源市", "通化市", "白山市", "松原市", "白城市"});
        PROVINCE_CITIES.put("山西省", new String[]{"太原市", "大同市", "阳泉市", "长治市", "晋城市", "朔州市", "晋中市", "运城市"});
        PROVINCE_CITIES.put("贵州省", new String[]{"贵阳市", "六盘水市", "遵义市", "安顺市", "毕节市", "铜仁市", "黔西南布依族苗族自治州", "黔东南苗族侗族自治州"});
        PROVINCE_CITIES.put("甘肃省", new String[]{"兰州市", "嘉峪关市", "金昌市", "白银市", "天水市", "武威市", "张掖市", "平凉市"});
        PROVINCE_CITIES.put("内蒙古自治区", new String[]{"呼和浩特市", "包头市", "乌海市", "赤峰市", "通辽市", "鄂尔多斯市", "呼伦贝尔市", "巴彦淖尔市"});
        PROVINCE_CITIES.put("新疆维吾尔自治区", new String[]{"乌鲁木齐市", "克拉玛依市", "吐鲁番市", "哈密市", "昌吉回族自治州", "博尔塔拉蒙古自治州", "巴音郭楞蒙古自治州", "阿克苏地区"});
        PROVINCE_CITIES.put("海南省", new String[]{"海口市", "三亚市", "三沙市", "儋州市"});
        PROVINCE_CITIES.put("宁夏回族自治区", new String[]{"银川市", "石嘴山市", "吴忠市", "固原市", "中卫市"});
        PROVINCE_CITIES.put("青海省", new String[]{"西宁市", "海东市", "海北藏族自治州", "黄南藏族自治州", "海南藏族自治州", "果洛藏族自治州", "玉树藏族自治州", "海西蒙古族藏族自治州"});
        PROVINCE_CITIES.put("西藏自治区", new String[]{"拉萨市", "日喀则市", "昌都市", "林芝市", "山南市", "那曲市", "阿里地区"});
    }

    /**
     * 🔥 改进的获取省份方法 - 基于IP段映射
     */
    public static String getProvince(String ip) {
        if (ip == null || !isValidIP(ip)) {
            return getRandomProvinceByWeight();
        }

        // 内网IP随机分配省份
        if (isInternalIP(ip)) {
            return getRandomProvinceByWeight();
        }

        // 根据IP段匹配省份
        for (Map.Entry<String, String[]> entry : PROVINCE_IP_RANGES.entrySet()) {
            String province = entry.getKey();
            String[] ranges = entry.getValue();

            for (String range : ranges) {
                if (isIPInRange(ip, range)) {
                    return province;
                }
            }
        }

        // 如果未匹配到已知IP段，基于IP特征进行判断
        return getProvinceByIPPattern(ip);
    }

    /**
     * 🔥 改进的获取城市方法 - 基于省份选择对应城市
     */
    public static String getCity(String ip) {
        String province = getProvince(ip);
        String[] cities = PROVINCE_CITIES.get(province);

        if (cities == null || cities.length == 0) {
            return province; // 如果是直辖市，城市就是省份
        }

        return cities[random.nextInt(cities.length)];
    }

    /**
     * 基于权重随机选择省份（备用方法）
     */
    private static String getRandomProvinceByWeight() {
        int totalWeight = PROVINCE_WEIGHTS.values().stream().mapToInt(Integer::intValue).sum();
        int randomWeight = random.nextInt(totalWeight) + 1;

        int currentWeight = 0;
        for (Map.Entry<String, Integer> entry : PROVINCE_WEIGHTS.entrySet()) {
            currentWeight += entry.getValue();
            if (randomWeight <= currentWeight) {
                return entry.getKey();
            }
        }

        return "广东省"; // 默认返回
    }

    /**
     * 根据IP模式识别省份
     */
    private static String getProvinceByIPPattern(String ip) {
        String[] ipParts = ip.split("\\.");
        if (ipParts.length != 4) {
            return getRandomProvinceByWeight();
        }

        try {
            int firstOctet = Integer.parseInt(ipParts[0]);
            int secondOctet = Integer.parseInt(ipParts[1]);

            // 基于IP段特征进行省份判断
            if (firstOctet == 1) {
                if (secondOctet >= 0 && secondOctet <= 1) return "北京市";
                else if (secondOctet >= 8 && secondOctet <= 15) return "天津市";
                else if (secondOctet >= 32 && secondOctet <= 63) return "上海市";
                else if (secondOctet >= 64 && secondOctet <= 127) return "重庆市";
                else if (secondOctet >= 128) return "广东省";
            } else if (firstOctet == 58) {
                return "江苏省"; // 58.x.x.x 主要分布在江苏
            } else if (firstOctet == 60) {
                return "浙江省"; // 60.x.x.x 主要分布在浙江
            } else if (firstOctet >= 110 && firstOctet <= 119) {
                String[] provinces = {"广东省", "江苏省", "浙江省", "山东省", "河南省"};
                return provinces[random.nextInt(provinces.length)];
            } else if (firstOctet >= 120 && firstOctet <= 125) {
                String[] provinces = {"广东省", "江苏省", "浙江省", "山东省"};
                return provinces[random.nextInt(provinces.length)];
            }
        } catch (NumberFormatException e) {
            // 如果解析失败，使用权重随机
        }

        // 默认基于权重随机
        return getRandomProvinceByWeight();
    }

    /**
     * 根据IP地址获取运营商
     */
    public static String getOperator(String ip) {
        if (ip == null || ip.isEmpty()) {
            return "未知运营商";
        }

        // 内网IP
        if (isInternalIP(ip)) {
            return "内网";
        }

        // 检查IP段匹配
        for (Map.Entry<String, String[]> entry : OPERATOR_IP_RANGES.entrySet()) {
            String operator = entry.getKey();
            String[] ranges = entry.getValue();

            for (String range : ranges) {
                if (isIPInRange(ip, range)) {
                    return operator;
                }
            }
        }

        // 如果未匹配到已知运营商，根据IP特征进行判断
        return getOperatorByIPPattern(ip);
    }

    /**
     * 判断IP是否为内网IP
     */
    private static boolean isInternalIP(String ip) {
        return ip.startsWith("10.") ||
                ip.startsWith("192.168.") ||
                (ip.startsWith("172.") && isInPrivateRange(ip)) ||
                ip.equals("127.0.0.1");
    }

    /**
     * 判断是否为172.16-31私有IP段
     */
    private static boolean isInPrivateRange(String ip) {
        String[] parts = ip.split("\\.");
        if (parts.length >= 2) {
            try {
                int secondOctet = Integer.parseInt(parts[1]);
                return secondOctet >= 16 && secondOctet <= 31;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * 判断IP是否在指定范围内
     */
    private static boolean isIPInRange(String ip, String range) {
        try {
            String[] parts = range.split("-");
            if (parts.length != 2) {
                return false;
            }

            String startIP = parts[0];
            String endIP = parts[1];

            long ipLong = ipToLong(ip);
            long startLong = ipToLong(startIP);
            long endLong = ipToLong(endIP);

            return ipLong >= startLong && ipLong <= endLong;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 将IP地址转换为长整型
     */
    private static long ipToLong(String ip) {
        String[] ipParts = ip.split("\\.");
        long result = 0;
        for (int i = 0; i < 4; i++) {
            result += Long.parseLong(ipParts[i]) << (24 - (8 * i));
        }
        return result;
    }

    /**
     * 根据IP模式识别运营商（备用方法）
     */
    private static String getOperatorByIPPattern(String ip) {
        String[] ipParts = ip.split("\\.");
        if (ipParts.length != 4) {
            return "未知运营商";
        }

        try {
            int firstOctet = Integer.parseInt(ipParts[0]);
            int secondOctet = Integer.parseInt(ipParts[1]);

            // 基于常见IP段进行判断
            if ((firstOctet == 1 || firstOctet == 14 || firstOctet == 27 ||
                    firstOctet == 36 || firstOctet == 49 || firstOctet == 58 ||
                    firstOctet == 60 || (firstOctet == 110 && secondOctet <= 127) ||
                    (firstOctet >= 218 && firstOctet <= 222))) {
                return "电信";
            } else if ((firstOctet == 42 || firstOctet == 43 || firstOctet == 58 ||
                    firstOctet == 59 || firstOctet == 60 || firstOctet == 61 ||
                    (firstOctet == 106) || (firstOctet >= 210 && firstOctet <= 211))) {
                return "联通";
            } else if ((firstOctet == 39 || firstOctet == 106 || firstOctet == 111 ||
                    firstOctet == 112 || firstOctet == 117 || firstOctet == 120 ||
                    firstOctet == 121 || firstOctet == 124 || firstOctet == 125 ||
                    firstOctet == 139 || firstOctet == 180 || firstOctet == 183 ||
                    firstOctet == 202)) {
                return "移动";
            } else {
                // 随机分配一个运营商
                String[] operators = {"电信", "联通", "移动", "铁通"};
                return operators[random.nextInt(operators.length)];
            }
        } catch (NumberFormatException e) {
            return "未知运营商";
        }
    }

    /**
     * 验证IP地址格式
     */
    public static boolean isValidIP(String ip) {
        if (ip == null || ip.isEmpty()) {
            return false;
        }

        String pattern = "^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
        return Pattern.matches(pattern, ip);
    }

    /**
     * 验证城市是否属于该省份
     */
    public static boolean isValidCityForProvince(String city, String province) {
        String[] validCities = PROVINCE_CITIES.get(province);
        if (validCities == null) return false;

        for (String validCity : validCities) {
            if (validCity.equals(city)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取省份的所有城市
     */
    public static String[] getCitiesByProvince(String province) {
        return PROVINCE_CITIES.get(province);
    }
}