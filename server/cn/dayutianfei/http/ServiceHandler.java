package cn.dayutianfei.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class ServiceHandler extends AbstractHandler{
	@Override
	public void handle(final String target, final Request request, final HttpServletRequest servlet_request, final HttpServletResponse response) throws IOException, ServletException {
		String opType = request.getParameter("op");
		String dosName = request.getParameter("schemaName");
		request.setHandled(true);
		response.setStatus(HttpStatus.OK_200);
		System.out.println(opType+ "  " + dosName);
		response.getWriter().write("tset");
		response.flushBuffer();
	}
}
