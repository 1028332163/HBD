package com.esoft.dp.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author taoshi 操作HDFS 工具类
 * 
 */
public class HDFSUtil {

	protected static Logger logger = LoggerFactory.getLogger(HDFSUtil.class);
	
	/**
	 * HDFS文件压缩格式使用类
	 */
	public static final String COMPRESS = "org.apache.hadoop.io.compress.BZip2Codec";

	private FileSystem fs;

	private Configuration conf;

	public void initConfig() throws Exception {
		conf = new Configuration();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
//		System.setProperty( "java.security.krb5.conf", Config.props.getProperty("kerberos.krb5.url"));
//		UserGroupInformation.setConfiguration(conf);
//		UserGroupInformation.loginUserFromKeytab("hdfs/master@HADOOP.COM", Config.props.getProperty("kerberos.hdfs.url"));
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"); 
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			logger.error("",e);
		}
	}

	/**
	 * 创建HDFS目录
	 * 
	 * @param path
	 *            创建的目录路径
	 * @return true:创建成功
	 * @throws IOException
	 */
	public boolean mkdir(String path) throws IOException {
		Path srcPath = new Path(path);
		return fs.mkdirs(srcPath);
	}

	/**
	 * 上传本地文件到HDFS
	 * 
	 * @param src
	 *            源文件路径
	 * @param dst
	 *            HDFS路径（目标路径）
	 * @throws IOException
	 */
	public void put(String src, String dst) throws IOException {
		Path srcPath = new Path(src);
		Path dstPath = new Path(dst); // 目标路径
		// 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认>为false
		fs.copyFromLocalFile(true, true, srcPath, dstPath);
		// FileUtil.copy(new File(src), fs, dstPath, false, conf);
	}

	/**
	 * 校验文件是否存在于HDFS中。
	 * 
	 * @param filePath
	 *            文件再HDFS中的路径
	 * @return true：存在
	 * @throws IOException
	 */
	public boolean check(String filePath) throws IOException {
		Path path = new Path(filePath);
		boolean isExists = fs.exists(path);
		return isExists;
	}

	/**
	 * 查看指定目录下的所有文件
	 * 
	 * @param filePath
	 *            HDFS目录路径
	 * @return 指定路径下所有文件列表
	 * @throws IOException
	 */
	public List<String> listFile(String filePath) throws IOException {
		List<String> listDir = new ArrayList<String>();
		Path path = new Path(filePath);
		FileStatus[] fileStatus = fs.listStatus(path);
		if (fileStatus.length != 0) {
			for (FileStatus file : fileStatus) {
				listDir.add(file.getPath().toString());
			}
			return listDir;
		}
		return listDir;
	}

	public boolean delete(String path) {
		Path p = new Path(path);
		URI uri = p.toUri();
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			return fs.delete(p, true);
		} catch (IOException e) {
			throw new HDFSIOException(path, conf.toString(), e);
		}
	}

	// 释放资源
	public void destroy() throws IOException {
		if (fs != null)
			fs.close();
	}

	public Map<String, String> simpleUpLoad(String localFile, String hdfsFile) {

		Map<String, String> result = new HashMap<String, String>();
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(localFile));
			FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
			//
			OutputStream out = fs.create(new Path(hdfsFile),
					new Progressable() {
						@Override
						public void progress() {
							logger.info(".");
						}
					});
			// OutputStream out = fs.create(new Path(hdfsFile));
			IOUtils.copyBytes(in, out, 4096, true);
			IOUtils.closeStream(in);
			result.put("resultStatus", "1");
			return result;
		} catch (IOException e) {
			logger.error("",e);
			result.put("resultStatus", "0");
			result.put("resultDesc", e.getMessage());
			return result;
		} finally {
			try {
				if(null != in){
					in.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error("",e);
			}
		}
	}

	/**
	 * 功能描述：拷贝一个目录或者文件到指定路径下，即把源文件拷贝到目标文件路径下(FileUtil)
	 * 
	 * @param source
	 *            源文件
	 * @param target
	 *            目标文件路径
	 * @return void
	 */
	public Map<String, String> copy(String localFile, String hdfsFile) {

		Map<String, String> result = new HashMap<String, String>();
		InputStream in = null;

		try {

			in = new BufferedInputStream(fs.open(new Path(localFile)));
			// reader = new BufferedReader(new InputStreamReader(fs.open(new
			// Path(localFile))));
			FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
			//
			OutputStream out = fs.create(new Path(hdfsFile),
					new Progressable() {
						@Override
						public void progress() {
							logger.info(".");
						}
					});
			// OutputStream out = fs.create(new Path(hdfsFile));
			IOUtils.copyBytes(in, out, 4096, true);
			IOUtils.closeStream(in);
			result.put("resultStatus", "1");
			return result;
		} catch (IOException e) {
			logger.error("",e);
			result.put("resultStatus", "0");
			result.put("resultDesc", e.getMessage());
			return result;
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error("",e);
			}
		}
	}

//	/**
//	 * 读取指定目录下的文件内容的第一行（表头）
//	 * 
//	 * @param filePath
//	 *            HDFS目录路径
//	 * @return 指定路径下所有文件列表
//	 * @throws IOException
//	 */
//	public String readFile(String filePath) throws IOException {
//		List<String> allFile = listFile(filePath);
//		if (allFile == null || allFile.size() == 0) {
//			return null;
//		} else {
//			String fileName = allFile.get(0);
//			List<String> listDir = new ArrayList<String>();
//			BufferedReader reader = null;
//			StringBuilder sb = new StringBuilder();
//			try {
//				reader = new BufferedReader(new InputStreamReader(
//						fs.open(new Path(fileName))));
//				String line = null;
//
//				if ((line = reader.readLine()) != null) {
//					// sb.append(line);
//					// String linearr[] = line.split(",");
//					// listDir.add(line);
//					return line;
//				}
//			} catch (Exception ioe) {
//				// System.out.println(fileName + " does't exist!");
//			} finally {
//				try {
//					reader.close();
//				} catch (IOException e) {
//					// System.out.println("Reader close failed");
//				}
//			}
//		}
//
//		return null;
//	}

	/**
	 * 修改HDFS目录的读写权限
	 * 
	 * @param src
	 *            要修改的HDFS目录
	 * @param mode
	 *            读写权限(例如：744)
	 * @throws IOException
	 */
	public void changePermission(Path src, String mode) throws IOException {
		// Path path = new Path(src);
		FsPermission fp = new FsPermission(mode);
		fs.setPermission(src, fp);
	}
	
	/**
	 * 压缩文件
	 * @param input (文件位置)
	 * @param output (生成的bz2文件，不用创建，需要指定)
	 * @throws Exception
	 */
    public void compress(String input,String output) throws Exception{
        Class<?> codecClass = Class.forName(HDFSUtil.COMPRESS);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        //指定压缩文件路径
        FSDataOutputStream outputStream = fs.create(new Path(output));
        //指定要被压缩的文件路径
        FSDataInputStream in = fs.open(new Path(input));
        //创建压缩输出流
        CompressionOutputStream out = codec.createOutputStream(outputStream);  
        IOUtils.copyBytes(in, out, conf); 
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

//	public static void main(String[] args) {
//		String srcPath = "E:\\41030402427.csv";
//		String descPath = "/out/17777.csv";
//		// 创建文件夹
//		try {
//			HDFSUtil hdfsAPI = new HDFSUtil();
//			// hdfsAPI.simpleUpLoad(srcPath, descPath);
//			// hdfsAPI.mkdir("/user/hadoop/test");
//			// hdfsAPI.put(srcPath, descPath);
//			hdfsAPI.check("hdfs://192.168.88.104:8020/out/correlations.csv");
//			// hdfsAPI.list("/out");
//			// for (String str : hdfsAPI.listFile("/examples")) {
//			// for (String str1 : hdfsAPI.listFile(str)) {// 连续调用2次
//			// System.out.println(str1);
//			// }
//			// }
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

	public FileSystem getFs() {
		return fs;
	}

	public void setFs(FileSystem fs) {
		this.fs = fs;
	}
}
