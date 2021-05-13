from jinja2 import Environment, FileSystemLoader
import os


class HtmlGenerator:
    env = Environment(loader=FileSystemLoader(os.path.dirname(os.path.realpath(__file__)) + '/html_template'))

    def __init__(self):
        pass

    @classmethod
    def generate_by_dict(cls, content_dict, result_path, template_name, result_name):
        template = cls.env.get_template(template_name)
        with open(os.path.join(result_path, result_name), 'w+') as fout:
            html_content = template.render(body=content_dict)
            fout.write(html_content)
        return os.path.join(result_path, result_name)