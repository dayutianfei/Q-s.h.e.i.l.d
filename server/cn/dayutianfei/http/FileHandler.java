package cn.dayutianfei.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class FileHandler extends AbstractHandler {

	private static final Log logger = LogFactory.getLog(FileHandler.class);

	@Override
	public void handle(final String target, final Request request, final HttpServletRequest servlet_request, final HttpServletResponse response) throws IOException, ServletException {
		this.doResponseHeaders(response, "text/plain");
		final InputStream inputStream = request.getInputStream();
		final int dataLength = request.getContentLength();
		ByteBuffer data = ByteBuffer.allocate(dataLength);
		while (data.hasRemaining()) {
			int p = data.position();
			int i = inputStream.read(data.array(), p, data.remaining());
			if (i < 0) {
				logger.warn("Unexpected EOF");
				response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
				response.getWriter().write("Unexpected EOF");
				return;
			}
			data.position(p + i);
		}
		data.flip();
		//String[] datas = null;
		try {
			
		} catch (Exception e) {
			response.getWriter().write("Unexpected Deserialize");
		}

	}

	/**
	 * Set the response headers. This method is called to set the response headers such as content type and content length. May be extended to add additional headers.
	 * 
	 * @param response
	 * @param resource
	 * @param mimeType
	 */
	protected void doResponseHeaders(final HttpServletResponse response, final String mimeType) {
		if (mimeType != null) {
			response.setContentType(mimeType);
		}
	}
}
