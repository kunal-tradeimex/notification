import { Injectable } from "@nestjs/common";
import * as HandleBars from 'handlebars';


@Injectable()
export class TemplateCompilerService {

    compile(templateStr: string,data: Record<string,any>): string {
        const template = HandleBars.compile(templateStr);
        return template(data);
    }

}