import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class Test1 {
    public static void main(String[] args){
        String urlPrefix = "http://localhost:8081/manage_self_clear_qg_add_new.jsp%3FPeType%3DS%26RoleValue%3DWildCard%26ListCodeSubType%3D%26ProfileEntryTypeValue%3DWildCard%26StatusValue%3DD%26SecRole_P%3DP%26PeType%3DS%26EntryType_S%3DS%26PeTypeScope%3DE%26ComplianceRegion%3D0%26PeTypeScope%3DE%26Index%3D0%26PASS_BACK%3DPASS_BACK%26FORM_CONTEXT%3DFORM_CONTEXT";
        String urlSufix = "?PeType=S&RoleValue=WildCard&ListCodeSubType=&ProfileEntryTypeValue=WildCard&StatusValue=D&SecRole_P=P&PeType=S&EntryType_S=S&PeTypeScope=E&ComplianceRegion=0&PeTypeScope=E&Index=0&PASS_BACK=PASS_BACK&FORM_CONTEXT=FORM_CONTEXT";
        String url1 = URLEncoder.encode(urlSufix, StandardCharsets.UTF_8);
        String url2 = URLEncoder.encode(url1, StandardCharsets.UTF_8);
        String url3 = URLEncoder.encode(url2, StandardCharsets.UTF_8);

        System.out.println(url1);
        System.out.println(url2);
        System.out.println(url3);
    }
}
