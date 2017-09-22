package test;

class Thread1 extends Thread{  
    private String name;  
    public Thread1(String name) {  
       this.name=name;  
    }  
    public void run() {  
        for (int i = 0; i < 5; i++) {  
            try {  
            	System.out.println(name + "运行  :  " + i);  
//                sleep((int) Math.random() * 10);  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
         
    }  
}  
public class ThreadTest {
	public static void main(String[] args) {  
        Thread1 mTh1=new Thread1("A");  
        Thread1 mTh2=new Thread1("B");  
        mTh1.start();  
        mTh2.start();  
  
    }  
}
