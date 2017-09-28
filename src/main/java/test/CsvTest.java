package test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import entities.ReIdAttributesTemp;
public class CsvTest {

	public static void main(String[] args) {
		File file=new File("E://a.csv");
		List<ReIdAttributesTemp> list=new ArrayList<>();
		ReIdAttributesTemp reIdAttributesTemp=new ReIdAttributesTemp();
		try {
			FileUtils.writeLines(file, list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
