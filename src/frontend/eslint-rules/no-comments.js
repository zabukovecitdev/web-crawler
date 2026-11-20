const DIRECTIVES = [/^\/\s*<reference\s/, /^\*\s*@type\s/]

const isDirective = (comment) => DIRECTIVES.some((directive) => directive.test(comment.value))

export default {
  meta: {
    type: 'suggestion',
    docs: { description: 'Disallow comments. Record decisions in docs/adr instead (ADR 0001).' },
    schema: [],
    messages: { comment: 'Comments are not allowed. Record the decision in docs/adr (see ADR 0001).' },
  },
  create(context) {
    return {
      Program() {
        for (const comment of context.sourceCode.getAllComments().filter((c) => !isDirective(c))) {
          context.report({ loc: comment.loc, messageId: 'comment' })
        }
      },
    }
  },
}
