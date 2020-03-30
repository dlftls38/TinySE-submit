package edu.hanyang.submit;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import edu.hanyang.indexer.Tokenizer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.ext.PorterStemmer;

public class TinySETokenizer implements Tokenizer {
	
	SimpleAnalyzer analyzer;
	PorterStemmer stemmer;
//	public TokenStream stream;
//	public CharTermAttribute term;
	
	
	public void setup() {
		analyzer = new SimpleAnalyzer();
		stemmer = new PorterStemmer();
	}
	
	public List<String> split(String text) {
		List<String> result = new ArrayList<String>();
		try {
			TokenStream stream = analyzer.tokenStream(null, new StringReader(text));
			stream.reset();
			CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
			while(stream.incrementToken()) {
				stemmer.setCurrent(term.toString());
				stemmer.stem();
				result.add(stemmer.getCurrent());
//				System.out.println(stemmer.getCurrent());
			}
			stream.close();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	public void clean() {
		analyzer.close();
	}

	public static void main(String[] args){
		TinySETokenizer yo = new TinySETokenizer();
		String text = "  He likes fried chicken keeping going went checked checking mapped mapping U.S.A USA  ACTIII III.III U.S.A state-of-the-art   ";
		String text2 = "i am a boy we are students analyzer analyzing analyzation";
		yo.setup();
		List<String> yoyo = yo.split(text2);
		System.out.println(yoyo);
	}
}